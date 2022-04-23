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
import msgpack
from zmq.eventloop.zmqstream import ZMQStream
from zmq.eventloop.ioloop import PeriodicCallback
from zmq.eventloop.ioloop import IOLoop
from random import choice

HB_INTERVAL = 1000  #: in milliseconds
HB_LIVENESS = 5  #: HBs to miss before connection counts as dead
MDP_CLIENT_VERSION = b'MDPC02'
MDP_WORKER_VERSION = b'MDPW02'


class Broker(object):
    """This is implementation of broker

    You don't need to override any methods in this class. It works immediately.
    Just call start_listening() method

    :type context:    Context
    :param context:   instance of zmq.Context
    :param endpoint:  listening address
    :type endpoint:   str
    """

    def __init__(self, context, endpoint):
        socket = context.socket(zmq.ROUTER)
        socket.bind(endpoint)
        self.stream = ZMQStream(socket)
        self.stream.on_recv(self.on_message)

        # services, workers and multicast groups
        self._workers = {}
        self._services = {}
        self._multicasts = {}
        self.hb_check_timer = PeriodicCallback(self.on_timer, HB_INTERVAL)
        self.hb_check_timer.start()
        return

    def start_listening(self):
        """Start listening to new messages
        """
        IOLoop.instance().start()

    def stop_listening(self):
        """Stop listening
        """
        IOLoop.instance().stop()

    def on_message(self, msg):
        """Processes given message.

        Decides what kind of message it is -- client or worker -- and
        calls the appropriate method. If unknown, the message is
        ignored.

        :param msg: message parts
        :type msg:  list of str

        :rtype: None
        """
        return_addresses, msg = self.split_address(msg)
        # dispatch on first frame after path
        method_to_call = None
        try:
            t = msg.pop(0)
            if t.startswith(b'MDPW'):
                method_to_call = self.on_worker
            elif t.startswith(b'MDPC'):
                method_to_call = self.on_client
            else:
                # Unknown protocol
                pass
        except (AttributeError, IndexError):
            # Wrong incoming msg format
            pass
        if method_to_call is not None:
            method_to_call(return_addresses, msg)
        return

    def on_client(self, return_addresses, message):
        """Method called on client message.

        Frame 0 of msg is the command id
        Frame 1 of msg is the requested service.
        The remaining frames are the request to forward to the worker.

        .. note::

           If the service is unknown to the broker the message is
           ignored.

        If a worker is available for the requested service, the
        message is repackaged and sent to the worker.

        If the service name starts with `mmi.`, the message is passed to
        the internal MMI_ handler.

        .. _MMI: http://rfc.zeromq.org/spec:8

        If the service name starts with `multicast.`, the message is sent
        to all worker in that group.

        :param return_addresses:    return address stack
        :type return_addresses:     list of str
        :param message:   message parts
        :type message:    list of str

        :rtype: None
        """

        cmd = message.pop(0)  # always 0x01
        service = message.pop(0)

        # mmi requests
        if service.startswith(b'mmi.'):
            self.on_client_mmi(return_addresses, service, message)
            return

        # multicast requests
        if service.startswith(b'multicast.'):
            self.on_client_multicast(return_addresses, service, message)
            return

        # worker requests
        try:
            available_workers = self._services[service]
            random_worker = choice(available_workers)  # TODO: loadbalancing
            to_send = [random_worker.id, b'', MDP_WORKER_VERSION, b'\x02']
            to_send.extend(return_addresses)
            to_send.append(b'')
            to_send.extend(message)
            self.stream.send_multipart(to_send)

        except KeyError:
            # unknwon service
            self.client_response(return_addresses, b'broker', b'No worker available', error=True)
        return

    def on_client_multicast(self, return_addresses, service, message):
        """Handling multicast messages from client

        :param return_addresses:    return address stack
        :type return_addresses:     list of str
        :param service:   name of mmi service
        :type service:    str
        :param message:   message parts
        :type message:    list of str
        """
        target = service[10:]  # remove 'multicast.'
        try:
            # first, prepare list of workers in target multicast
            grouped_by_names = {}
            for worker in self._multicasts[target]:
                if worker.service in grouped_by_names:
                    grouped_by_names[worker.service].append(worker)
                else:
                    grouped_by_names[worker.service] = [worker]

            # send message to one worker per service
            sent_messages = []
            for name, workers in grouped_by_names.items():
                random_worker = choice(workers)  # TODO: loadbalancing
                to_send = [random_worker.id, b'', MDP_WORKER_VERSION, b'\x02']
                to_send.extend(return_addresses)
                to_send.append(b'')
                to_send.extend(message)
                self.stream.send_multipart(to_send)
                sent_messages.append(random_worker.service)

            # notify client with list of services in multicast group
            client_msg = return_addresses[:]
            client_msg.extend([b'', MDP_WORKER_VERSION, b'\x05'])
            client_msg.extend(sent_messages)
            self.stream.send_multipart(client_msg)
        except KeyError:
            # unknwon service
            self.client_response(return_addresses, b'broker', b'No services available in this multicast', error=True)
        return

    def on_client_mmi(self, return_addresses, service, message):
        """Handling MMI messages from client

        :param return_addresses:    return address stack
        :type return_addresses:     list of str
        :param service:   name of mmi service
        :type service:    str
        :param message:   message parts
        :type message:    list of str
        """
        if service == b'mmi.service':
            return self.on_client_mmi_service(return_addresses, service, message)
        elif service == b'mmi.services':
            return self.on_client_mmi_services(return_addresses, service, message)
        elif service == b'mmi.workers':
            return self.on_client_mmi_workers(return_addresses, service, message)
        elif service == b'mmi.multicasts':
            return self.on_client_mmi_multicasts(return_addresses, service, message)
        else:
            # unknown mmi service - notify client
            self.client_response(return_addresses, b'broker', b'Service not found', error=True)

    def on_client_mmi_service(self, return_addresses, service, message):
        """Check if services exists
        """
        return self.client_response_pack(return_addresses, b'broker', message[0] in self._services.keys())

    def on_client_mmi_services(self, return_addresses, service, message):
        """List of all services
        """
        return self.client_response_pack(return_addresses, b'broker', [k for k in self._services])

    def on_client_mmi_workers(self, return_addresses, service, message):
        """Number of workers per service
        """
        s = {}
        for se in self._services:
            s[se] = len(self._services[se])
        return self.client_response_pack(return_addresses, b'broker', s)

    def on_client_mmi_multicasts(self, return_addresses, service, message):
        """List of available multicast groups
        """
        m = {}
        for se in self._multicasts:
            m[se] = [s.service for s in self._multicasts[se]]
        return self.client_response_pack(return_addresses, b'broker', m)

    def on_worker(self, return_addresses, message):
        """Method called on worker message.

        Frame 0 of msg is the command id.
        The remaining frames depend on the command.

        This method determines the command sent by the worker and
        calls the appropriate method. If the command is unknown the
        message is ignored and a DISCONNECT is sent.

        :param return_addresses:  return address stack
        :type return_addresses:   list of str
        :param message: message parts
        :type message:  list of str

        :rtype: None
        """
        cmd = message.pop(0)

        worker_cmds = {
            b'\x01': self.on_worker_ready,
            b'\x03': self.on_worker_partial_reply,
            b'\x04': self.on_worker_final_reply,
            b'\x05': self.on_worker_heartbeat,
            b'\x06': self.on_worker_disconnect,
            b'\x07': self.on_worker_multicast_add,  # this is not part of the Majordomo Protocol 0.2 !
            b'\x08': self.on_worker_exception,  # this is not part of the Majordomo Protocol 0.2 !
            b'\x09': self.on_worker_error,  # this is not part of the Majordomo Protocol 0.2 !
        }
        if cmd in worker_cmds:
            fnc = worker_cmds[cmd]
            fnc(return_addresses, message)
        else:
            # ignore unknown command
            pass
        return

    def on_worker_ready(self, return_addresses, message):
        """Called when new worker is ready to receive messages.

        Register worker to list of available services.

        :param return_addresses:  return address stack
        :type return_addresses:   list of str
        :param message: message parts
        :type message:  list of str

        :rtype: None
        """
        # Frame 0 of msg is a service.
        service = message.pop(0)
        wid = return_addresses[0]
        return self.register_worker(wid, service)

    def on_worker_partial_reply(self, return_addresses, message):
        """Process worker PARTIAL REPLY command.

        Route the `message` to the client given by the address(es) in front of `message`.

        :param return_addresses:  return address stack
        :type return_addresses:   list of str
        :param message: message parts
        :type message:  list of str

        :rtype: None
        """
        ret_id = return_addresses[0]
        try:
            wrep = self._workers[ret_id]
        except KeyError:
            return  # worker is gone, ignore this message

        return_addresses, msg = self.split_address(message)

        self.client_response(return_addresses, wrep.service, msg, partial=True)
        return

    def on_worker_final_reply(self, return_addresses, message):
        """Process worker FINAL REPLY command.

        Route the `message` to the client given by the address(es) in front of `message`.

        :param return_addresses:  return address stack
        :type return_addresses:   list of str
        :param message: message parts
        :type message:  list of str

        :rtype: None
        """
        ret_id = return_addresses[0]
        try:
            wrep = self._workers[ret_id]
        except KeyError:
            return  # worker is gone, ignore this message

        return_addresses, msg = self.split_address(message)

        self.client_response(return_addresses, wrep.service, msg)
        return

    def on_worker_heartbeat(self, return_addresses, message):
        """Process worker HEARTBEAT command.

        :param return_addresses:  return address stack
        :type return_addresses:   list of str
        :param message: message parts
        :type message:  list of str

        :rtype: None
        """
        ret_id = return_addresses[0]
        try:
            worker = self._workers[ret_id]
            if worker.is_alive():
                worker.on_heartbeat()
        except KeyError:
            # ignore HB for unknown worker
            pass
        return

    def on_worker_disconnect(self, return_addresses, message):
        """Process worker DISCONNECT command.

        Remove worker from list of services.

        :param return_addresses:  return address stack
        :type return_addresses:   list of str
        :param message: message parts
        :type message:  list of str

        :rtype: None
        """
        wid = return_addresses[0]
        return self.unregister_worker(wid)

    def on_worker_multicast_add(self, return_addresses, message):
        """Process worker MULTICAST ADD command.

        Add worker to list of multicasts
        This is not part of the Majordomo Protocol 0.2 !

        :param return_addresses:  return address stack
        :type return_addresses:   list of str
        :param message: message parts
        :type message:  list of str

        :rtype: None
        """
        multicast_name = message.pop(0)
        wid = return_addresses[0]
        return self.register_multicast(wid, multicast_name)

    def on_worker_exception(self, return_addresses, message):
        """Process worker EXCEPTION command.

        Route the `message` to the client given by the address(es) in front of `message`.
        This is not part of the Majordomo Protocol 0.2 !

        :param return_addresses:  return address stack
        :type return_addresses:   list of str
        :param message: message parts
        :type message:  list of str

        :rtype: None
        """
        ret_id = return_addresses[0]
        try:
            wrep = self._workers[ret_id]
        except KeyError:
            return  # worker is gone, ignore this message

        return_addresses, msg = self.split_address(message)

        self.client_response(return_addresses, wrep.service, msg, exception=True)
        return

    def on_worker_error(self, return_addresses, message):
        """Process worker ERROR command.

        Route the `message` to the client given by the address(es) in front of `message`.
        This is not part of the Majordomo Protocol 0.2 !

        :param return_addresses:  return address stack
        :type return_addresses:   list of str
        :param message: message parts
        :type message:  list of str

        :rtype: None
        """
        ret_id = return_addresses[0]
        try:
            wrep = self._workers[ret_id]
        except KeyError:
            return  # worker is gone, ignore this message

        return_addresses, msg = self.split_address(message)

        self.client_response(return_addresses, wrep.service, msg, error=True)
        return

    def on_timer(self):
        """Method called on timer expiry.

        Checks which workers are dead and unregisters them.

        :rtype: None
        """
        for wrep in list(self._workers.values()):
            if not wrep.is_alive():
                self.on_log_event('worker.connection_timeout',
                                  "Worker connection timeout for service '%s'." % wrep.service)
                self.unregister_worker(wrep.id)
        return

    def client_response(self, return_addresses, service, msg, partial=False, exception=False, error=False):
        """Package and send reply to client.

        :param return_addresses:       return address stack
        :type return_addresses:        list of str
        :param service:  name of service
        :type service:   str
        :param msg:      message parts
        :type msg:       list of str | str

        :rtype: None
        """
        to_send = return_addresses[:]
        if error:
            t = b'\x04'
        elif exception:
            t = b'\x06'
        elif partial:
            t = b'\x02'
        else:
            t = b'\x03'
        to_send.extend([b'', MDP_WORKER_VERSION, t, service])
        if isinstance(msg, list):
            to_send.extend(msg)
        else:
            to_send.append(msg)
        self.stream.send_multipart(to_send)
        return

    def client_response_pack(self, return_addresses, service, msg, partial=False):
        """Send message to client and pack it (msg) in msgpack format

        Exception and error messages are not allowed here.

        :param return_addresses:       return address stack
        :type return_addresses:        list of str
        :param service:  name of service
        :type service:   str
        :param msg:      message to pack and send
        :type msg:       mixed
        :param partial:  if message is partial of final, default False
        :type partial:   bool

        :rtype: None
        """
        packed = msgpack.Packer().pack(msg)
        self.client_response(return_addresses, service, packed, partial=partial)

    def unregister_worker(self, wid):
        """Unregister the worker with the given id.

        If the worker id is not registered, nothing happens.

        Will stop all timers for the worker.

        :param wid:    the worker id.
        :type wid:     str

        :rtype: None
        """
        try:
            wrep = self._workers[wid]
        except KeyError:
            # not registered, ignore
            return
        wrep.shutdown()
        service = wrep.service

        # remove worker from service list
        if service in self._services:
            worker_list = self._services[service]
            for worker in worker_list:
                if worker.id == wid:
                    worker_list.remove(worker)
            if not worker_list:
                del self._services[service]

        # remove worker from multicasts
        for m_name in self._multicasts:
            mw = self._multicasts[m_name]
            for w in [w for w in mw if w.id == wid]:
                mw.remove(w)

        # delete empty rows
        empty_keys = [k for k, v in self._multicasts.items() if len(v) == 0]
        for k in empty_keys:
            del self._multicasts[k]

        del self._workers[wid]
        self.on_log_event('worker.unregister', "Worker for service '%s' disconnected." % service)
        return

    def register_worker(self, wid, service):
        """Register the worker id and add it to the given service.

        Does nothing if worker is already known.

        :param wid:    the worker id.
        :type wid:     str
        :param service:    the service name.
        :type service:     str

        :rtype: None
        """
        if wid in self._workers:
            return
        worker = WorkerRep(wid, service, self.stream)
        self._workers[wid] = worker

        if service in self._services:
            s = self._services[service]
            s.append(worker)
        else:
            self._services[service] = [worker]
        self.on_log_event('worker.register', "Worker for service '%s' is connected." % service)
        return

    def register_multicast(self, wid, multicast_name):
        """Add worker to multicast group

        :type wid:       str
        :param wid:      the worker id.
        :type multicast_name:  str
        :param multicast_name: group name
        """
        if wid not in self._workers:
            return
        worker = self._workers[wid]
        if multicast_name in self._multicasts:
            m = self._multicasts[multicast_name]
            m.append(worker)
        else:
            self._multicasts[multicast_name] = [worker]
        self.on_log_event('worker.register_multicast',
                          "Service '%s' added to multicast group '%s'." % (worker.service, multicast_name))
        return

    def shutdown(self):
        """Shutdown broker.

        Will unregister all workers, stop all timers and ignore all further
        messages.

        .. warning:: The instance MUST not be used after :func:`shutdown` has been called.

        :rtype: None
        """
        self.stream.on_recv(None)
        self.stream.socket.setsockopt(zmq.LINGER, 0)
        self.stream.socket.close()
        self.stream.close()
        self.stream = None

        self._workers = {}
        self._services = {}
        self._multicasts = {}
        return

    def on_log_event(self, event, message):
        """Override this method if you want to log events from broker

        :type event:    str
        :param event:   event type - used for filtering
        :type message:  str
        :param message: log message

        :rtype: None
        """
        pass

    # helpers:
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


class WorkerRep(object):
    """Helper class to represent a worker in the broker.

    Instances of this class are used to track the state of the attached worker
    and carry the timers for incomming and outgoing heartbeats.

    :type wid:       str
    :param wid:      the worker id.
    :param service:  service this worker serves
    :type service:   str
    :param stream:   the ZMQStream used to send messages
    :type stream:    ZMQStream
    """

    def __init__(self, wid, service, stream):
        self.id = wid
        self.service = service
        self.multicasts = []
        self.curr_liveness = HB_LIVENESS
        self.stream = stream
        self.last_hb = 0
        self.hb_out_timer = PeriodicCallback(self.send_hb, HB_INTERVAL)
        self.hb_out_timer.start()
        return

    def send_hb(self):
        """Called on every HB_INTERVAL.

        Decrements the current liveness by one.

        Sends heartbeat to worker.
        """
        self.curr_liveness -= 1
        msg = [self.id, b'', MDP_WORKER_VERSION, b'\x05']
        self.stream.send_multipart(msg)
        return

    def on_heartbeat(self):
        """Called when a heartbeat message from the worker was received.

        Sets current liveness to HB_LIVENESS.
        """
        self.curr_liveness = HB_LIVENESS
        return

    def is_alive(self):
        """Returns True when the worker is considered alive.
        """
        return self.curr_liveness > 0

    def shutdown(self):
        """Cleanup worker.

        Stops timer.
        """
        self.hb_out_timer.stop()
        self.hb_out_timer = None
        self.stream = None
        return
