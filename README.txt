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

Choose one deployment mode before configuring an application:

  Standalone
      Do not configure SwarmCache, SwarmSessionManager,
      SwarmNonSessionManager or SwarmIDGenerator. No JGroups channel is
      created by HelmaSwarm.

  Legacy swarm
      Configure the required HelmaSwarm implementations but leave
      swarm.join.strict unset or set it to false. This preserves historical
      startup behaviour and does not provide the fail-closed guarantees below.

  Strict session member
      Use SwarmSessionManager with memberRole=session. The process may serve
      user traffic only after strict join and initial session synchronization.

  Strict non-session member
      Use SwarmNonSessionManager with memberRole=non-session. This is intended
      for a process that participates in the view but must neither provide nor
      consume replicated user sessions.

Setting swarm.join.strict=false disables only strict startup. It does not
disable HelmaSwarm. To run standalone, all HelmaSwarm implementation properties
must be absent from the effective app.properties. Leaving SwarmCache or
SwarmIDGenerator enabled is enough to create/use a swarm channel even when the
default Helma session manager is selected.

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

Strict Startup and Session State
================================

Legacy behaviour remains the default. A newly created swarm can opt into the
strict startup protocol explicitly. Do not mix legacy and strict members under
the same swarm.name.

Application member:

  swarm.join.strict = true
  swarm.join.dbSource = iiefs
  swarm.join.validationQuery = SELECT 1
  swarm.join.validationQueryTimeoutSeconds = 2
  swarm.join.maxConnectTimeoutMillis = 2000
  swarm.join.retryInitialDelayMillis = 1000
  swarm.join.retryMaxDelayMillis = 5000
  swarm.join.maxAttempts = 0
  swarm.join.minViewSize = 2
  swarm.join.minViewWaitMillis = 10000
  swarm.session.stateProviderMode = strict
  swarm.session.memberRole = session
  swarm.session.discoveryTimeoutMillis = 2000
  swarm.session.discoveryRetryDelayMillis = 500
  swarm.session.stateTransferTimeoutMillis = 5000
  swarm.session.stateMaxBytes = 67108864
  swarm.session.stateMaxEntries = 100000
  swarm.session.bootstrapBufferMaxMessages = 10000
  swarm.session.bootstrapBufferMaxBytes = 16777216
  swarm.session.bootstrapBufferMaxEntries = 100000
  sessionManagerImpl = helma.swarm.SwarmSessionManager

Non-session member, for example a dedicated sync process:

  swarm.session.memberRole = non-session
  sessionManagerImpl = helma.swarm.SwarmNonSessionManager

Strict startup validates the application database before creating a fresh
JGroups channel and publishes it only after the configured minimum view has
been reached. A positive MySQL connectTimeout no greater than
swarm.join.maxConnectTimeoutMillis is required in the selected application
database URL. JDBC_PING may use a separate shared database; its connection must
have independent bounded connectTimeout and socketTimeout values. Strict
startup probes the selected application database first and then creates a fresh
JGroups channel. A JDBC_PING failure that produces only a singleton cannot pass
a configured minimum view greater than one and the complete attempt is retried.
swarm.join.validationQueryTimeoutSeconds must be greater than zero; JDBC defines
zero as an unbounded query timeout and strict mode rejects it.

maxAttempts=0 retries without an attempt limit. A positive maxAttempts caps
real join attempts and then keeps the application fail-closed until the process
is stopped. Ordinary thread interrupts never make a strict application ready.

Strict session members accept state only from an explicitly SESSION_READY
provider. State messages are correlated by nonce, view and provider, validated
completely, and committed atomically before readiness. Live replication is
buffered during bootstrap and replayed afterwards. On a cold start the first
session member in view order is the seed. With persistentSessions=true that
seed keeps its disk state; joining members treat the ready provider as
authoritative and replace their local disk snapshot before becoming ready.
The cold seed validates its persistent state against the same byte and entry
limits before publishing SESSION_READY. Replies correlated to an older retry
round are ignored and cannot invalidate the active round.
State export and transfer stop at the configured serialized-byte and entry
limits, and imports recheck both limits before committing state. Live session
messages retain the same byte and entry bounds after initialization. The live
bootstrap buffer independently caps message count, estimated payload-data
bytes and retained entry count. Limit violations keep the member unready and
restart the complete discovery/transfer round. A violation after initialization
clears session readiness, demotes the member from SESSION_READY and requires a
process restart before it can serve traffic or provider state again.

Where Each Setting Belongs
==========================

app.properties contains the application behaviour and role:

  cacheImpl = helma.swarm.SwarmCache
  sessionManagerImpl = helma.swarm.SwarmSessionManager
  idGeneratorImpl = helma.swarm.SwarmIDGenerator
  swarm.name = application-environment
  swarm.conf = /mounted/config/swarm.conf
  swarm.jgroups.stack = jdbc-ping
  swarm.join.strict = true
  swarm.session.stateProviderMode = strict
  swarm.session.memberRole = session

memberRole and sessionManagerImpl must be configured together in the same
effective application properties. Strict session members require
SwarmSessionManager; strict non-session members require SwarmNonSessionManager.
A mismatch is invalid and remains fail-closed.

db.properties contains the database source selected by swarm.join.dbSource.
Strict startup currently requires a MySQL-compatible jdbc:mysql URL containing
a positive connectTimeout no greater than swarm.join.maxConnectTimeoutMillis.
Database credentials belong in the platform secret mechanism, not in the
repository examples.

swarm.conf contains only JGroups protocol stacks and cache-domain definitions.
It determines transport, bind address/port and discovery, for example TCP with
JDBC_PING. swarm.jgroups.stack in app.properties selects one named stack from
that file.

The swarm.conf shipped in this repository is a development example. Its UDP
stack uses multicast and its TCP stack binds to 127.0.0.1 with TCPPING. A
container deployment must supply a reviewed stack that binds to a reachable
interface and normally uses JDBC_PING with bounded connect/socket timeouts.
The effective production JDBC_PING file and its secrets must be supplied by the
deployment; they are intentionally not embedded in this repository.

In standalone mode swarm.conf may remain present on disk, but it is not used as
long as no HelmaSwarm implementation is configured. Remove dormant swarm.*
properties from the standalone app.properties as well, so a later partial
configuration change cannot activate stale settings accidentally.

In legacy mode swarm.conf is active and the selected discovery stack is used.
memberRole and strict state-transfer settings are ignored because strict
startup is disabled. Legacy mode can still use a view-size readiness check, but
it does not close the startup race in which a failed JDBC_PING discovery forms
a connected singleton.

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

Monitoring and Readiness
========================

HelmaSwarm exposes read-only channel status helpers through
helma.swarm.ChannelUtils:

  isConnected(app)
  getViewSize(app)
  getView(app)
  isMaster(app)
  isJoinStateAvailable(app)
  getJoinStatus(app)
  getJoinAttempts(app)
  getJoinLastError(app)
  isSessionStateAvailable(app)
  isSessionStateInitialized(app)
  getSessionStateStatus(app)
  getSessionStateProvider(app)
  getKnownSessionStateProviders(app)
  getLastReceivedStateSessionCount(app)
  getSessionStateLastError(app)
  isControlProtocolComplete(app)

These helpers only inspect the existing JGroups channel. They do not create a
new channel and do not trigger a swarm join. This makes them suitable for
application-level monitoring and readiness endpoints.

Applications that use these helpers should keep Swarm-aware readiness
explicitly configurable. Non-Swarm deployments must not require these helpers
or a connected swarm channel. For iiEFS/San Pedro this is controlled by the
application property monitoring.swarm.required.

Platform Compatibility
======================

The strict bootstrap and session protocol are Java/JGroups functionality and
do not call OpenShift or Kubernetes APIs. They can therefore run on ARO,
OpenShift, Kubernetes or Docker Swarm when the surrounding runtime supplies the
same networking, database, configuration, secret and health-check capabilities.

On Kubernetes, replace OpenShift BuildConfig/ImageStream/DeploymentConfig/Route
objects with the corresponding build pipeline, OCI registry, Deployment,
Service and Ingress or Gateway. Mount equivalent app.properties, db.properties
and swarm.conf inputs, allow the JGroups transport and JDBC_PING database path,
and use the same HTTP health endpoints as Kubernetes probes.

On Docker Swarm, the Java and JDBC_PING paths can run on an overlay network.
The service must supply configs/secrets, reachable JGroups ports, stable shared
database discovery and an image healthcheck. Docker Swarm health and routing
semantics are not identical to Kubernetes readiness, so the selected ingress or
load balancer must be verified to stop routing to an unready task.

The implementation is portable, but only its local Java-8 and JDBC_PING
integration paths have been verified. ARO/OpenShift acceptance remains a
release gate; Kubernetes and Docker Swarm require separate deployment manifests
and acceptance tests. Platform portability also does not close the AUTH and
constrained-wire-format security gate.

Credits & Feedback
==================

HelmaSwarm is written by Hannes Wallnoefer (hannes at helma dot at).
This sofware was initially inspired by SwarmCache <http://swarmcache.sf.net/>,
from which it borrows part of its name.
