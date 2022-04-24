#!/usr/bin/python
__author__ = 'Matej Capkovic'
__email__ = 'capkovic@gmail.com'

from mdprpc import Worker
from zmq import Context
from zmq.eventloop.ioloop import IOLoop
import signal
import traceback
import argparse

# include all modules from modules directory
import modules

methods = {}
for name, m in [(name, modules.__dict__[name]) for name in modules.__all__]:
    for method in dir(m):
        if hasattr(m.__dict__.get(method), '__call__'):
            methods[name + '.' + method] = m.__dict__.get(method)


def params_convert(_args, _kwargs):
    if _args and _kwargs:
        return _args, _kwargs
    elif _args:
        return _args
    elif _kwargs:
        return _kwargs
    else:
        return tuple()


class MyWorker(Worker):
    max_forks = 50

    def on_log_event(self, event, message):
        print(message)

    def do_work(self, addresses, name, args, kwargs):
        try:
            try:
                print('starting job \'%s\'' % name)
                method_to_call = methods[name.decode('utf-8')]
            except KeyError:
                method_to_call = None
            if not method_to_call:
                print('called unknownd method \'%s\'' % name)
                raise Exception('method %s not found' % name)

            self.send_reply(addresses, 'started', partial=True)
            param = params_convert(args, kwargs)
            if bool(param):
                result = method_to_call(param)
            else:
                result = method_to_call()
        except BaseException as e:
            print('exception in job \'%s\'' % name)
            print(traceback.format_exc())
            ex = {
                'class': type(e).__name__,
                'message': format(e),
                'traceback': traceback.format_exc()
            }
            self.send_reply(addresses, ex, exception=True)
            return

        print('finishing job \'%s\'' % name)
        self.send_reply(addresses, result)


if __name__ == '__main__':
    # arg parser
    parser = argparse.ArgumentParser(description='Worker')
    parser.add_argument('target', metavar='tcp://127.0.0.1:5550', type=str,
                        help='target IP and port ex. tcp://127.0.0.1:5550')
    parser.add_argument('name', metavar='name', type=str, help='worker name')
    parser.add_argument('--groups', metavar='name', type=str, nargs='+', help='list of groups')
    args = parser.parse_args()

    context = Context()

    # handle exit signals
    def handler(signum, frame):
        print('worker is exiting, received signum: %s' % signum)
        IOLoop.instance().stop()


    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)

    print('starting worker \'%s\' connected to IP \'%s\'' % (args.name, args.target))
    worker = MyWorker(context, args.target, args.name, [] if args.groups is None else args.groups)
    IOLoop.instance().start()
    worker.shutdown()
