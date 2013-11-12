__license__ = """
    This file is part of MDPRPC.

    MDPRPC is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    MDPRPC is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with MDPRPC.  If not, see <http://www.gnu.org/licenses/>.
"""

__author__ = 'Matej Capkovic'
__email__ = 'capkovic@gmail.com'

import zmq
from zmq.eventloop.zmqstream import ZMQStream
from zmq.eventloop.ioloop import IOLoop, DelayedCallback, PeriodicCallback
from uuid import uuid4
from multiprocessing import Process
import msgpack

MDP_WORKER_VERSION = b'MDPW02'


class Worker(object):

    """Class for the MDP worker side.

    Thin encapsulation of a zmq.DEALER socket.
    Provides a send method with optional timeout parameter.

    Will use a timeout to indicate a broker failure.
    """
    max_forks = 10

    ipc = 'ipc:///tmp/zmq-rpc-'+str(uuid4())
    HB_INTERVAL = 1000  # in milliseconds
    HB_LIVENESS = 3    # HBs to miss before connection counts as dead

    def __init__(self, context, endpoint, service, multicasts=()):
        """Initialize the MDPWorker.

        :param context:    is the zmq context to create the socket from
        :type context:     zmq.Context
        :param service:    service name - you can put hostname here
        :type service:     str
        :param multicasts: list of groups to subscribe
        :type multicasts:  list
        """
        self.context = context
        self.endpoint = endpoint
        self.service = service.encode('utf-8')  # convert to byte-string - required in python 3
        self.multicasts = [m.encode('utf-8') for m in multicasts]  # convert to byte-string
        self.stream = None
        self._tmo = None
        self.need_handshake = True
        self.ticker = None
        self._delayed_cb = None
        self._create_stream()
        self.forks = []
        self.curr_liveness = self.HB_LIVENESS

        socket = self.context.socket(zmq.ROUTER)
        socket.bind(self.ipc)
        self.stream_w = ZMQStream(socket)
        self.stream_w.on_recv(self._on_fork_response)
        self.reply_socket = None
        return

    def _create_stream(self):
        """Helper to create the socket and the stream.
        """
        self.on_log_event('broker.connect', 'Trying to connect do broker')
        socket = self.context.socket(zmq.DEALER)
        ioloop = IOLoop.instance()
        self.stream = ZMQStream(socket, ioloop)
        self.stream.on_recv(self._on_message)
        self.stream.socket.setsockopt(zmq.LINGER, 0)
        self.stream.connect(self.endpoint)
        self.ticker = PeriodicCallback(self._tick, self.HB_INTERVAL)
        self._send_ready()
        for m in self.multicasts:
            self._register_worker_to_multicast(m)
        self.ticker.start()
        return

    def _tick(self):
        """Method called every HB_INTERVAL milliseconds.
        """
        self.curr_liveness -= 1
        self.send_hb()
        if self.curr_liveness >= 0:
            return
        # ouch, connection seems to be dead
        self.on_log_event('broker.timeout', 'Connection to broker timeouted, disconnecting')
        self.shutdown(False)
        # try to recreate it
        self._delayed_cb = DelayedCallback(self._create_stream, 5000)
        self._delayed_cb.start()
        return

    def send_hb(self):
        """Construct and send HB message to broker.
        """
        msg = [b'', MDP_WORKER_VERSION, b'\x05']
        self.stream.send_multipart(msg)
        return

    def shutdown(self, final=True):
        """Method to deactivate the worker connection completely.

        Will delete the stream and the underlying socket.

        :param final: if shutdown is final and we want to close all sockets
        :type final:  bool
        """

        if self.ticker:
            self.ticker.stop()
            self.ticker = None
        if not self.stream:
            return

        self.stream.on_recv(None)
        self.disconnect()
        self.stream.socket.close()
        self.stream.close()
        self.stream = None
        self.need_handshake = True

        if final:
            self.stream_w.socket.close()
            self.stream_w.close()
            self.stream = None
        return

    def disconnect(self):
        """Helper method to send the workers DISCONNECT message.
        """
        self.stream.socket.send_multipart([b'', MDP_WORKER_VERSION, b'\x06' ])
        self.curr_liveness = self.HB_LIVENESS
        return

    def _send_ready(self):
        """Helper method to prepare and send the workers READY message.
        """
        self.on_log_event('broker.ready', 'Sending ready to broker.')
        ready_msg = [b'', MDP_WORKER_VERSION, b'\x01', self.service]
        self.stream.send_multipart(ready_msg)
        self.curr_liveness = self.HB_LIVENESS
        return

    def _register_worker_to_multicast(self, name):
        """Helper method to register worker to multicast group

        :param name:  group name
        :type name:   str
        """
        self.on_log_event('broker.register-group', 'Subscribing to group \'%s\'.' % name)
        reg_msg = [b'', MDP_WORKER_VERSION, b'\x07', name]
        self.stream.send_multipart(reg_msg)
        self.curr_liveness = self.HB_LIVENESS
        return

    def _on_message(self, msg):
        """Helper method called on message receive.

        :param msg:  message parts
        :type msg:   list
        """
        # 1st part is empty
        msg.pop(0)
        # 2nd part is protocol version
        protocol_version = msg.pop(0)
        if protocol_version != MDP_WORKER_VERSION:  # version check, ignore old versions
            return
        # 3rd part is message type
        msg_type = msg.pop(0)
        # any message resets the liveness counter
        self.need_handshake = False
        self.curr_liveness = self.HB_LIVENESS
        if msg_type == b'\x06':  # disconnect
            self.curr_liveness = 0  # reconnect will be triggered by hb timer
        elif msg_type == b'\x02':  # request
            # remaining parts are the user message
            addresses, msg = self.split_address(msg)
            self._on_request(addresses, msg)
        elif msg_type == b'\x05':
            # received hardbeat - timer handled above
            pass
        else:
            # invalid message ignored
            pass
        return

    def _on_fork_response(self, to_send):
        """Helper method to send message from forked worker.
        This message will be received by main worker process and resend to broker.

        :param to_send  address and data to send
        :type to_send   list
        """
        self.stream.send_multipart(to_send)
        return

    def send_reply(self, addresses, msg, partial=False, exception=False):
        """Send reply from forked worker process.
        This method can be called only from do_work() method!
        This method will send messages to main worker listening on local socket in /tmp/zmq-rpc-...

        :param addresses: return address stack
        :type addresses:  list of str
        :param msg:       return value from called method
        :type msg:        mixed
        :param partial:   if the message is partial or final
        :type partial:    bool
        :param exception: if the message is exception, msg format is: {'class':'c', 'message':'m', 'traceback':'t'}
        :type exception:  bool
        """
        if not self.reply_socket:
            context = zmq.Context()
            self.reply_socket = context.socket(zmq.DEALER)
            self.reply_socket.connect(self.ipc)
        msg = msgpack.Packer().pack(msg)
        if exception:
            to_send = [b'', MDP_WORKER_VERSION, b'\x08']
        elif partial:
            to_send = [b'', MDP_WORKER_VERSION, b'\x03']
        else:
            to_send = [b'', MDP_WORKER_VERSION, b'\x04']
        to_send.extend(addresses)
        to_send.append(b'')
        if isinstance(msg, list):
            to_send.extend(msg)
        else:
            to_send.append(msg)
        m = self.reply_socket.send_multipart(to_send, track=True, copy=False)
        m.wait()
        if not partial:
            self.reply_socket.close()
            self.reply_socket = None
        return

    def send_message(self, addresses, msg, partial=False, error=False):
        """Send response message from main worker process.
        Please do not call this method from do_work()

        :param addresses: return address stack
        :type addresses:  list of str
        :param msg:       return value from called method
        :type msg:        mixed
        :param partial:   if the message is partial or final
        :type partial:    bool
        :param error:     if the message is error
        :type error:      bool
        """
        to_send = [b'', MDP_WORKER_VERSION]
        if partial:
            to_send.append(b'\x03')
        elif error:
            to_send.append(b'\x09')
        else:
            to_send.append(b'\x04')
        to_send.extend(addresses)
        to_send.append(b'')
        if isinstance(msg, list):
            to_send.extend(msg)
        else:
            to_send.append(msg)
        self.stream.send_multipart(to_send)
        return

    def _on_request(self, addresses, message):
        """Helper method called on RPC message receive.
        """
        # remove finished forks
        self._remove_finished_processes()
        # test max forks
        if len(self.forks) >= self.max_forks:
            self.send_message(addresses, b'max workers limit exceeded', error=True)
            self.on_max_forks(addresses, message)
            return

        name = message[0]
        args = msgpack.unpackb(message[1])
        kwargs = msgpack.unpackb(message[2])

        p = Process(target=self.do_work, args=(addresses, name, args, kwargs))
        p.start()
        p._args = None  # free memory
        self.forks.append(p)
        return

    def _remove_finished_processes(self):
        """Helper method dedicated to cleaning list of forked workers
        """
        for f in [f for f in self.forks if not f.is_alive()]:
            self.forks.remove(f)
        return

    def split_address(self, msg):
        """Function to split return Id and message received by ROUTER socket.

        Returns 2-tuple with return Id and remaining message parts.
        Empty frames after the Id are stripped.
        """
        ret_ids = []
        for i, p in enumerate(msg):
            if p:
                ret_ids.append(p)
            else:
                break
        return ret_ids, msg[i + 1:]

    def on_log_event(self, event, message):
        """Override this method if you want to log events from broker

        :type event:    str
        :param event:   event type - used for filtering
        :type message:  str
        :param message: log message

        :rtype: None
        """
        pass

    def on_max_forks(self, addresses, message):
        """This method is called when max_forks limit is reached
        You can override this method.
        """
        pass

    def do_work(self, addresses, name, args, kwargs):
        """Main method responsible for handling rpc calls, and sending response messages.
         Please override this method!

        :param addresses: return address stack
        :type addresses:  list of str
        :param name:      name of task
        :type name:       str
        :param args:      positional task arguments
        :type args:       list
        :param kwargs:    key-value task arguments
        :type kwargs:     dict
        """
        # this is example of simple response message
        self.send_reply(addresses, 'method not implemented')  # and send message to main worker
        # you can also send partial message and exception - read 'send_reply' docs
        return
