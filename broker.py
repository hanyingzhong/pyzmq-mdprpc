#!/usr/bin/python
__author__ = 'Matej Capkovic'
__email__ = 'capkovic@gmail.com'

from mdprpc import Broker
from zmq import Context
import signal
import argparse


class MyBroker(Broker):
    # you can override methods here
    def on_log_event(self, event, message):
        print(message)

if __name__ == '__main__':

    # arg parser
    parser = argparse.ArgumentParser(description='Worker')
    parser.add_argument('endpoint', metavar='tcp://127.0.0.1:5550', type=str, help='listen on IP and port ex. tcp://127.0.0.1:5550')
    args = parser.parse_args()

    # initialize broker
    context = Context()
    broker = MyBroker(context, args.endpoint)

    # handle exit signals
    def handler(signum, frame):
        broker.stop_listening()  # disabling main loop
    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)

    # start listening
    print('broker started')
    broker.start_listening()  # main loop
    print('broker is shutting down')
    broker.shutdown()
