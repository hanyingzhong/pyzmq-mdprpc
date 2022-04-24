pyzmq-mdprpc
===================================================
https://rfc.zeromq.org/spec/7/

pyzmq-mdprpc is RPC implementation based on zeromq and msgpack and inspired by pyzmq-mdp. This repository contains library that can be used programmatically via lib APIs or from command line using the example client.py/broker.py/worker.py commands.

Main features are:

 - light-weight and fast
 - broker-based, one connection point for all workers and clients
 - no queue, all calls are executed immediately
 - can run one task on multiple workers identified by a group name
 - workers are resilient to memory leaks (fork)
 - worker can send multiple responses (work progress, etc.)

If you want to use this library without example scripts, install it with the command:

    pip install git+ssh://git@github.com:capkovic/pyzmq-mdprpc.git

Or you can clone the repository and try following examples:

Create a broker with a one-liner
-----------------------------------------------
First, start broker listening on TCP port `5550`, accepting connections on IP address `127.0.0.1`

    python broker.py tcp://127.0.0.1:5550

This port is common for all workers and clients.

Run workers
---------------------
    python worker.py tcp://127.0.0.1:5550 worker1

Start the task from client
----------------------------------
    python client.py tcp://127.0.0.1:5550 worker1 test.ping

`worker1` is the name of the worker and `time.sleep` is the method to call


Run the tasks on multiple workers
--------------------------------------------
At first, start 2 workers and assign them to multicast group `group1` and one to `group2`

    python worker.py tcp://127.0.0.1:5550 worker1 --groups group1
    python worker.py tcp://127.0.0.1:5550 worker2 --groups group1 group2

Run task on all workers from `group1` and `group2`:

    python client.py tcp://127.0.0.1:5550 multicast.group1 test.ping
    python client.py tcp://127.0.0.1:5550 multicast.group2 test.ping

Getting the list of all running workers and multicast groups
------------------------------------------------
    python client.py tcp://127.0.0.1:5550 mmi.services
    python client.py tcp://127.0.0.1:5550 mmi.multicasts

Writing tasks
----------------------
Tasks are automaticaly loaded from modules directory. Exceptions and errors are caught and rethrown at the clients' side. The task name is composed of the module name and the function name separated by a dot. For example `test.ping` will call method `ping` in module `test.py`.
