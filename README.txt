HelmaSwarm
==========

HelmaSwarm is a Helma extension that allows multiple instances of Helma to be
combined into a cluster. It consists of three tools that plug into various parts
of Helma:

  1) SwarmCache, which acts as a replacement object cache that propagates
     notifications of changed objects to other members in the swarm.

  2) SwarmSessionManager, which is a replacement for the Helma session
     manager that replicates sessions among swarm members and propagates
     session updates and expiry.

  3) SwarmIDGenerator, which is necessary to coordinate primary key generation
     for new persistent objects when the underlying database uses SELECT max(id)
     for creating primary keys (as is usually the case with MySQL). It is not
     needed if Sequences are used for key generation (as usually the case with
     Oracle, for instance).

HelmaSwarm uses JGroups <http://jgroups.org> for communication between Helma
instances. This version of HelmaSwarm comes bundled with JGroups 2.12.3.Final.

Asynchronous Communication and Sticky Sessions
==============================================

HelmaSwarm uses asynchronous communication for keeping consistent state among
Helma instances. This means that messages are sent without the sender waiting
for confirmation of receipt by other swarm members. While this greatly reduces
group communication overhead and complexity, it makes it possible that a client
that has altered some state on one Helma instance may still see the old state in
a subsequent request to another swarm member, if that request gets ahead of the
swarm notification of the state change.

For this reason it is advisable (although not strictly necessary) to use sticky
sessions with HelmaSwarm clusters. See doc/README-Apache.txt for information on
how to implement sticky sessions with Apache and mod_jk. Round Robin DNS should
minimize the risk of this scenario, since DNS names aren't refreshed that often,
and if they are chances should be small that DNS lookup and HTTP request gets
ahead of HelmaSwarm communication on the server LAN.

Requirements
============

This version of HelmaSwarm requires Helma 1.7.0-rc3 (Release Candidate 3)
or later.

Known Bugs
==========

Because of an incompatible change in the Session class, session transfer at 
startup time will not work between instances of Helma 1.6.2 and earlier 
Helma 1.6 releases in the same swarm. 

Building
========

HelmaSwarm is built with Apache Ant. 

  1) Edit build.properties to match your Helma installation directory.

  2) Run the following in the command line:

       ant install

This should compile and build helmaswarm-version.jar and copy it to the lib/ext 
directory of your Helma installation along with the JGroups jar file.

Configuration
=============

To enable SwarmCache, SwarmSessionManager, and SwarmIDGenerator for a Helma
application, add the respective lines to its app.properties file:

  cacheImpl = helma.swarm.SwarmCache
  sessionManagerImpl = helma.swarm.SwarmSessionManager
  idGeneratorImpl = helma.swarm.SwarmIDGenerator

HelmaSwarm uses a group name to identify and connect to a particular swarm. By
default, the application name is used as the group name. If you want use a
different group name, for instance because your swarm is made up of
applications with different names, you can set the HelmaSwarm group name with
the swarm.name entry in app.properties:

  swarm.name = mySwarmName

HelmaSwarm uses an XML configuration file from which it reads its properties.
This file is called swarm.conf and is either set by just copying it to the
application directory, or by setting the helma.conf app property:

  swarm.conf = /path/to/helmaswarm/swarm.conf

The most important setting in helma.conf is the JGroups network stack. By
default, HelmaSwarm uses a UDP multicast stack called "udp". helma.swarm also
contains a TCP stack. The JGroups stack is configured with the
following app property:

  swarm.jgroups.stack = [udp|tcp|custom]

The default UDP multicast stack uses port 22024 on multicast address
224.0.0.132. It is advisable to use a different setting if multiple swarm
instances are operated on the same local network to avoid unnecessary network
traffic.

Startup-only Join Retry
=======================

The normal legacy behavior creates one JGroups channel and attempts one join.
It remains active when swarm.join.startupRetry is absent, or is false, and no
startup retry settings are present.

The startup-only retry is enabled as one complete app.properties profile:

  swarm.join.startupRetry = true
  swarm.join.minViewSize = 2
  swarm.join.minViewWaitMillis = 10000
  swarm.join.retryInitialDelayMillis = 1000
  swarm.join.retryMaxDelayMillis = 60000

Each failed connection or startup view timeout is closed completely before a
fresh channel is created. The adapter is shared with SwarmCache,
SwarmIDGenerator, and SwarmSessionManager only after the minimum startup view
has been reached. Once published, a later loss of members does not replace,
demote, or disconnect the adapter. Runtime quorum is deliberately outside this
feature.

For a standalone member, startupRetry may be enabled with minViewSize = 1. Its
initial session-state lookup keeps the legacy single-attempt behavior. A
clustered deployment with minViewSize greater than 1 retries a completed
negative state transfer, but never starts a second transfer while the callback
of an already accepted transfer is still pending.

minViewSize accepts 1 through 32, minViewWaitMillis 100 through 60000, and
retryInitialDelayMillis 1000 through 60000. retryMaxDelayMillis is fixed at
60000 to bound the long-running retry rate. Startup tunables without an
explicit startupRetry = true, old STRICT settings, and partial mixtures of
legacy and retry profiles are invalid. Invalid startup configuration blocks
application startup until process shutdown instead of allowing a partially
initialized application.

Enable, disable, and roll back the complete profile atomically. Do not mix
members with different startup profiles or different session-manager behavior
inside one swarm.

The Java 8 verification commands are:

  ant clean test
  ant process-test
  HELMA_SWARM_IT=1 ant integration-test

Monitoring and Readiness
========================

HelmaSwarm exposes read-only channel status helpers through
helma.swarm.ChannelUtils:

  isConnected(app)
  getViewSize(app)
  getView(app)
  isMaster(app)

These helpers only inspect the existing JGroups channel. They do not create a
new channel and do not trigger a swarm join. This makes them suitable for
application-level monitoring and readiness endpoints.

Applications that use these helpers should keep Swarm-aware readiness
explicitly configurable. Non-Swarm deployments must not require these helpers
or a connected swarm channel. For iiEFS/San Pedro this is controlled by the
application property monitoring.swarm.required.

Credits & Feedback
==================

HelmaSwarm is written by Hannes Wallnoefer (hannes at helma dot at).
This sofware was initially inspired by SwarmCache <http://swarmcache.sf.net/>,
from which it borrows part of its name.
