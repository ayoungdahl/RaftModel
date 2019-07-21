# RaftModel
An implementation and simulation of the Raft distributed concensus protocol.

This project was inspired by https://raft.github.io/

To play with this model:

cd into raft_proj; sbt; run to execute the model

When run a number of raft nodes will form a group and a client
will send a monotonically increasing sequence of Ints to the 
group.  The group will establish a consensus on which Ints have
been applied.  In "CHAOS" mode (see below) raft nodes will drop
out intermittently so not all Ints will be committed, but all nodes
and the client will agree on which ones were.

The client will always try a random node first, and then retry
at a suggested leader.  If no leader is present the client will
give up on that particular message and move on to the next.

Once a prescribed number of Ints have been tried (currently config-ed)
to 100, but changeable in application.conf) the client will
terminate the system after displaying the sequence of Ints
it believes have been commited -- as informed by whatever raft node
was leader at the time.

Each group node will display their commit sequence everytime it is
updated.

These can be compared.
  
In this model there is only one provided client, however the model
should work with any number of clients.  I left it at one because
the display is busy enough as it is.

Also, all actors in this model are in one system (no remotes), however
the model should work with remote systems.


in raft_proj/src/main/resources/application.conf there are model parms.

Of particular note is the "mode" parameter.  This is packaged with
mode = 1, which causues raft nodes in the model to randomly go 
"on hiatus".  While on hiatus they will not respond to or initiate
messages, except if they are leader they will accept client requests
without forwarding them.

If mode = 0, the model will run without any simulated disruptions

There are also timer parameters (base and variance) to adjust the 
frequency of different events.  I think the current settings make
for a quick enough but sane simulation, but feel free to play!

The raft nodes will write their state to disk, so if write 
permissions are not had on the directory there will be problems.
