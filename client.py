#!/usr/bin/python
__author__ = 'Matej Capkovic'
__email__ = 'capkovic@gmail.com'

from mdprpc import Client
from zmq import Context
import signal
import argparse
import sys


class ConsoleClient(Client):

    def on_partial_message(self, worker, msg):
        print("partial msg from '%s': %s" % (worker.decode('utf-8'), repr(msg)))

    def on_message(self, worker, msg):
        print("final msg from '%s': %s" % (worker.decode('utf-8'), repr(msg)))

    def on_timeout(self):
        no_responses = []
        if self.multicast_services:
            no_responses = [i for i in self.multicast_services if i not in self.multicast_already_received]
        if no_responses:
            print("Timeout! No responses from: %s" % repr(no_responses))
        else:
            print('Timeout!')

    def on_error_message(self, worker, msg):
        print("Error from '%s' with message: %s" % (worker.decode('utf-8'), msg.decode('utf-8')))

    def on_exception_message(self, worker, cls, message, traceback):
        print("%s from '%s' with message: %s" % (cls.decode('utf-8'), worker.decode('utf-8'), message.decode('utf-8')))
        print(traceback.decode('utf-8'))

    def on_multicast_start(self, services):
        print('starting tasks on: %s' % repr(services))

if __name__ == '__main__':
    # arg parser
    parser = argparse.ArgumentParser(description='Run task')
    parser.add_argument('ip', metavar='tcp://127.0.0.1:5550', type=str, help='target IP and port ex. tcp://10.0.0.1:5550')
    parser.add_argument('target', metavar='worker name', type=str, help='target worker name')
    parser.add_argument('task', default='', nargs='?', metavar='task', type=str, help='name of the task')
    parser.add_argument('--args', metavar='args', type=str, required=False, help='arguments')
    parser.add_argument('--kwargs', metavar='kwargs', type=str, required=False, help='key-value arguments')
    parser.add_argument('--timeout', metavar='timeout', type=float, required=False, default=10.0, help='request timeout is sec, default 10')
    args = parser.parse_args()
    arguments = eval('[%s]'%args.args if args.args is not None else '[]')
    kwarguments = eval('{%s}'%args.kwargs if args.kwargs is not None else '{}')
    timeout = args.timeout
    if not args.target.startswith('mmi.') and args.task == '':
        sys.exit('please specify task name')

    # client
    context = Context()
    client = ConsoleClient(context, args.ip, args.target)

    # handle exit signals
    def handler(signum, frame):
        client.stop_waiting()
    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)

    # rpc call
    client.rpc(args.task, arguments, kwarguments, timeout)
    client.wait_for_reply()
    client.shutdown()
