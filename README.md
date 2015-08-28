[![Build Status](https://travis-ci.org/ErikDubbelboer/discnt.svg)](https://travis-ci.org/ErikDubbelboer/discnt)

Discnt, in-memory, distributed counters
===

**WARNING: This is alpha code NOT suitable for production. The implementation and API will likely change in significant ways during the next months. The code and algorithms are not tested enough. A lot more work is needed.**

Setup
===

To play with Discnt please do the following:

1. Compile Discnt, if you can compile Redis, you can compile Discnt, it's the usual no external deps thing. Just type `make`. Binaries (`discnt-cli` and `discnt-server`) will end up in the `src` directory.
2. Run a few Discnt nodes in different ports. Create different `discnt.conf` files following the example `discnt.conf` in the source distribution.
3. After you have them running, you need to join the cluster. Just select a random node among the nodes you are running, and send the command `CLUSTER MEET <ip> <port>` for every other node in the cluster.

To run a node, just call `./discnt-server`.

For example if you are running three Discnt servers in port 7711, 7712, 7713 in order to join the cluster you should use the `discnt-cli` command line tool and run the following commands:

    ./discnt-cli -p 7711 cluster meet 127.0.0.1 7712
    ./discnt-cli -p 7711 cluster meet 127.0.0.1 7713

Your cluster should now be ready. You can try to add a job and fetch it back
in order to test if everything is working:

    ./discnt-cli -p 7711
    127.0.0.1:7711> INCRBY test 0.1
    0.1
    127.0.0.1:7711> GET test
    0.1

Remember that you can increment counters on different nodes as Discnt
is multi master.

API
===

Discnt API is composed of a small set of commands, since the system solves a
single very specific problem. The main commands are:

    INCR   counter_name
    INCRBY counter_name amount
    DECR   counter_name
    DECRBY counter_name amount

    GET counter_name

And to get and set the per counter prediction precision:

    PRECISION counter_name <value>

Other commands
===

Note: not everything implemented yet.

    INFO
Generic server information / stats.

    HELLO
Returns hello format version, this node ID, all the nodes IDs, IP addresses,
ports, and priority (lower is better, means node more available).
Clients should use this as an handshake command when connecting with a
Discnt node.

    KEYS patten
Similar to redis KEYS.

Client libraries
===

Discnt uses the same protocol as Redis itself. To adapt Redis clients, or to use it directly, should be pretty easy. However note that Discnt default port is 5262 and not 6379.

FAQ
===

Is Discnt part of Redis?
---

No, it is a standalone project, however a big part of the Redis networking source code, nodes message bus, libraries, and the client protocol, were reused in this new project.

However while it is a separated project, conceptually Discnt is related to Redis, since it tries to solve a Redis use case in a vertical, ad-hoc way.

Who created Discnt?
---

Discnt is a side project of Erik Dubbelboer.

What does Discnt means?
---

DIStributed CouNTers.

