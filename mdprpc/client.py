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
from zmq.eventloop.ioloop import IOLoop, DelayedCallback
import msgpack

MDP_CLIENT_VERSION = b'MDPC02'
MDP_WORKER_VERSION = b'MDPW02'


class InvalidStateError(RuntimeError):
    """Exception raised when the requested action is not available due to socket state.
    """
    pass


class Client(object):

    def __init__(self, context, endpoint, service):
        """Initialize the Client.
        """
        socket = context.socket(zmq.DEALER)
        self.ioloop = IOLoop.instance()
        self.service = service.encode('utf-8')
        self.endpoint = endpoint
        self.stream = ZMQStream(socket, self.ioloop)
        self.stream.on_recv(self._on_message)
        self.can_send = True
        self._proto_prefix = [MDP_CLIENT_VERSION, b'\x01', service.encode('utf-8')]
        self._tmo = None
        self.timed_out = False
        self.multicast_services = None
        self.multicast_already_received = []
        socket.connect(endpoint)
        return

    def shutdown(self):
        """Method to deactivate the client connection completely.

        Will delete the stream and the underlying socket.

        .. warning:: The instance MUST not be used after :func:`shutdown` has been called.

        :rtype: None
        """
        if not self.stream:
            return
        self.stream.socket.setsockopt(zmq.LINGER, 0)
        self.stream.socket.close()
        self.stream.close()
        self.stream = None
        return

    def request(self, msg, timeout=None):
        """Send the given message.

        :param msg:     message parts to send.
        :type msg:      list of str
        :param timeout: time to wait in milliseconds.
        :type timeout:  int | float

        :rtype None:
        """
        if not self.can_send:
            raise InvalidStateError()
        try:
            if type(msg) in (bytes, unicode):  # python 2
                msg = [msg]
        except NameError:
            if type(msg) == bytes:  # python 3
                msg = [msg]
        # prepare full message
        to_send = [b'']
        to_send.extend(self._proto_prefix[:])
        to_send.extend(msg)
        self.stream.send_multipart(to_send)
        self.can_send = False
        self.multicast_services = None
        if timeout:
            self._start_timeout(timeout)
        return

    def rpc(self, method, args=None, kwargs=None, timeout=None):
        """Call RPC method

        :param method:  name of method
        :type method:   str
        :param args:    list of args
        :type args:     list
        :param kwargs:  list of kwargs
        :type kwargs:   dict
        :param timeout: time to wait in milliseconds.
        :type timeout:  int | float

        :rtype None:
        """
        data = [method.encode('utf-8'), msgpack.packb([] if args is None else args), msgpack.packb({} if kwargs is None else kwargs)]
        return self.request(data, timeout)

    def _on_message(self, message):
        """Helper method called on message receive.

        :param message:   list of message parts.
        :type message:    list of str
        """
        message.pop(0) # remove empty string
        protocol_version = message.pop(0)
        if protocol_version != MDP_WORKER_VERSION:  # version check, ignore old versions
            return
        message_type = message.pop(0)

        if message_type == b'\x02':  # partial message
            worker = message.pop(0)
            try:
                msg = msgpack.unpackb(message[0])
            except:
                msg = message[0]

            self.on_partial_message(worker, msg)
        elif message_type == b'\x03':  # final message
            worker = message.pop(0)
            try:
                msg = msgpack.unpackb(message[0])
            except:
                msg = message[0]
            self.on_message(worker, msg)

            if message[0] not in self.multicast_already_received:
                self.multicast_already_received.append(worker)
            if self.multicast_services is None or len(self.multicast_already_received) == len(self.multicast_services):
                self.stop_waiting()
        elif message_type == b'\x04':  # error message
            worker = message.pop(0)
            self.on_error_message(worker, message[0])
            self.stop_waiting()
        elif message_type == b'\x05':  # multicast start, load list of multicast services
            self.multicast_services = message
            self.multicast_already_received = []
            self.on_multicast_start(message)
        elif message_type == b'\x06':  # exception message
            worker = message.pop(0)
            try:
                msg = msgpack.unpackb(message[0])
            except:
                msg = {b'class': 'Exception', b'message': 'parsing failed', b'traceback': message[0]}
            self.on_exception_message(worker, msg[b'class'], msg[b'message'], msg[b'traceback'])

            if message[0] not in self.multicast_already_received:
                self.multicast_already_received.append(worker)
            if self.multicast_services is None or len(self.multicast_already_received) == len(self.multicast_services):
                self.stop_waiting()
        #else: unknown type - do nothing
        return

    def wait_for_reply(self):
        """Start waiting for replies
        """
        self.ioloop.start()

    def stop_waiting(self):
        """Stop waiting for replies and cancel timeout
        """
        self.ioloop.stop()
        self.can_send = True
        self._stop_timeout()

    def _on_timeout(self):
        """Helper called after timeout.
        """
        self.timed_out = True
        self._tmo = None
        self.ioloop.stop()
        self.on_timeout()
        return

    def _start_timeout(self, timeout):
        """Helper for starting the timeout.

        :param timeout:  the time to wait in milliseconds.
        :type timeout:   int
        """
        self._tmo = DelayedCallback(self._on_timeout, timeout)
        self._tmo.start()
        return

    def _stop_timeout(self):
        """Helper for stopping timeout event
        """
        if self._tmo:
            self._tmo.stop()
            self._tmo = None

    def on_message(self, service, msg):
        """Public method called when a message arrived.

        .. note:: Does nothing. Should be overloaded!

        :param service:  name of the worker
        :type service:   str
        :param msg:      message
        :type msg:       mixed
        """
        pass

    def on_partial_message(self, service, msg):
        """Public method called when a partial message arrived.

        .. note:: Does nothing. Should be overloaded!

        :param service:  name of the worker
        :type service:   str
        :param msg:      message
        :type msg:       mixed
        """
        pass

    def on_error_message(self, worker, msg):
        """Public method called when an error message arrived.

        .. note:: Does nothing. Should be overloaded!

        :param service:  name of the worker
        :type service:   str
        :param msg:      message
        :type msg:       str
        """
        pass

    def on_exception_message(self, service, cls, message, traceback):
        """Public method called when an exception arrived.

        .. note:: Does nothing. Should be overloaded!

        :param service:  name of the worker
        :type service:   str
        :param cls:      exception class name
        :type cls:       str
        :param message:  error message
        :type message:   str
        :param traceback: traceback
        :type traceback:  str
        """
        pass

    def on_timeout(self):
        """Public method called when a timeout occured.

        .. note:: Does nothing. Should be overloaded!
        """
        pass

    def on_multicast_start(self, services):
        """Public method called when multicast request started

        .. note:: Does nothing. Should be overloaded!

        :param services:  list of services in multicast group
        :type services:   list
        """
        pass
