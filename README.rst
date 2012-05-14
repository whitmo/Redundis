==========
 Redundis
==========

Tools for assisting with Redis Redundancy

Watcher
=======

A script that will supervise a chain of 3 redis instance behind
haproxy providing simple failover and recovery.::

 $ dundis watch --redi=localhost:6379,localhost:6380,localhost:6381 

The watcher keeps track of each redis instance, whether it is up or
down, and helps the chain maintain the best possible state of
replication.  The watcher manages weights within haproxy, so that upon
a failure, new connections will go to the next redis in the chain.
When a redis instance returns from failure, the watcher will put it at
the end of the chain.



Set up
------

Run haproxy off a config that looks like::

 defaults
   contimeout  5000
   clitimeout  50000
   srvtimeout  50000

 global
   stats socket /tmp/redundis-haproxy.sock level admin
   log   127.0.0.1 local1 debug

 listen redis :6679
   server redis-6391 localhost:6379 check weight 150
   server redis-6393 localhost:6380 check weight 1
   server redis-6392 localhost:6381 check weight 0


Redis clients should attach to redis via haproxy. For a client running
on the same box as haproxy in this case: `localhost:6679`.

The watcher needs access to the unix socket (defaults to
`/tmp/redundis-haproxy.sock`).  This means you will need to run
haproxy and the watcher on the same machine (or set up some sort of
tunnel perhaps using `socat`).


Caveats
-------

This scheme provides reasonable continuouity of service for
redis, but no promises of data integrity.  If a master or slave
crashes during replication or between syncs, data will be lost.


TODO
====

 - more tests
 - docs
 - failover for watcher
