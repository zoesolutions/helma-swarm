package helma.swarm;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import helma.framework.core.Session;
import helma.scripting.ScriptingEngineInterface;
import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.View;
import org.jgroups.blocks.PullPushAdapter;
import org.jgroups.util.UUID;

final class SwarmSessionManagerLifecycleTest {

    private SwarmSessionManagerLifecycleTest() {
    }

    static void register(AllTests.Suite suite) {
        suite.reproduction("strict bootstrap buffers immutable copies of every live payload",
                new AllTests.CheckedRunnable() {
                    public void run() throws Exception {
                        SwarmSessionManager.BootstrapBuffer buffer =
                                new SwarmSessionManager.BootstrapBuffer(3);
                        byte[] bytes = new byte[] {1, 2};
                        SwarmSessionManager.SessionUpdate update =
                                new SwarmSessionManager.SessionUpdate("update", 7L);
                        update.debugBuffer = new StringBuffer("before");
                        update.cacheNode = new byte[] {3, 4};
                        Object[] ids = new Object[] {"one", "two"};
                        SwarmSessionManager.SessionIdList idList =
                                new SwarmSessionManager.SessionIdList(
                                        SwarmSessionManager.DISCARD, ids);

                        buffer.add(bytes);
                        buffer.add(update);
                        buffer.add(idList);
                        bytes[0] = 9;
                        update.debugBuffer.append("-after");
                        update.cacheNode[0] = 9;
                        ids[0] = "changed";

                        List payloads = buffer.snapshot();
                        AllTests.assertEquals(Byte.valueOf((byte) 1),
                                Byte.valueOf(((byte[]) payloads.get(0))[0]),
                                "byte payload retained sender-owned storage");
                        SwarmSessionManager.SessionUpdate copied =
                                (SwarmSessionManager.SessionUpdate) payloads.get(1);
                        AllTests.assertEquals("before", copied.debugBuffer.toString(),
                                "update debug buffer was not copied");
                        AllTests.assertEquals(Byte.valueOf((byte) 3),
                                Byte.valueOf(copied.cacheNode[0]),
                                "update cache bytes were not copied");
                        SwarmSessionManager.SessionIdList copiedIds =
                                (SwarmSessionManager.SessionIdList) payloads.get(2);
                        AllTests.assertEquals("one", copiedIds.ids[0],
                                "session id list retained sender-owned array");
                    }
                });

        suite.reproduction("strict bootstrap buffer overflow is discarded before retry",
                new AllTests.CheckedRunnable() {
                    public void run() throws Exception {
                        final SwarmSessionManager.BootstrapBuffer buffer =
                                new SwarmSessionManager.BootstrapBuffer(1);
                        buffer.add(new byte[] {1});
                        try {
                            buffer.add(new byte[] {2});
                        } catch (IllegalStateException expected) {
                            AllTests.assertTrue(buffer.isOverflowed(),
                                    "overflow did not become a terminal buffer condition");
                            buffer.resetForRetry();
                            AllTests.assertTrue(!buffer.isOverflowed(),
                                    "fresh retry retained terminal overflow state");
                            buffer.add(new byte[] {3});
                            List retryPayloads = buffer.snapshot();
                            AllTests.assertEquals(Integer.valueOf(1),
                                    Integer.valueOf(retryPayloads.size()),
                                    "fresh retry retained payloads from the failed round");
                            AllTests.assertEquals(Byte.valueOf((byte) 3),
                                    Byte.valueOf(((byte[]) retryPayloads.get(0))[0]),
                                    "fresh retry did not accept a new live payload");
                            return;
                        }
                        throw new AssertionError("overflow was accepted");
                    }
                });

        suite.reproduction("strict bootstrap buffer enforces byte and entry limits",
                new AllTests.CheckedRunnable() {
                    public void run() throws Exception {
                        final SwarmSessionManager.BootstrapBuffer bytes =
                                new SwarmSessionManager.BootstrapBuffer(3, 4, 3);
                        bytes.add(new byte[] {1, 2, 3});
                        expectBufferOverflow(new AllTests.CheckedRunnable() {
                            public void run() {
                                bytes.add(new byte[] {4, 5});
                            }
                        }, "bootstrap buffer exceeded its configured byte limit");

                        final SwarmSessionManager.BootstrapBuffer entries =
                                new SwarmSessionManager.BootstrapBuffer(3, 1024, 2);
                        expectBufferOverflow(new AllTests.CheckedRunnable() {
                            public void run() {
                                entries.add(new SwarmSessionManager.SessionIdList(
                                        SwarmSessionManager.TOUCH,
                                        new Object[] {"one", "two", "three"}));
                            }
                        }, "bootstrap buffer exceeded its configured entry limit");
                    }
                });

        suite.reproduction("strict state export stops serialization at the byte limit",
                new AllTests.CheckedRunnable() {
                    public void run() throws Exception {
                        ScriptingEngineInterface engine = (ScriptingEngineInterface)
                                Proxy.newProxyInstance(
                                        ScriptingEngineInterface.class.getClassLoader(),
                                        new Class[] {ScriptingEngineInterface.class},
                                        new InvocationHandler() {
                                            public Object invoke(Object proxy, Method method,
                                                                 Object[] args)
                                                    throws Exception {
                                                if ("serialize".equals(method.getName())) {
                                                    ((OutputStream) args[1]).write(
                                                            new byte[] {1, 2, 3, 4, 5});
                                                }
                                                return null;
                                            }
                                        });
                        try {
                            SwarmSessionManager.objectToBytes("state", engine, 4);
                        } catch (IOException expected) {
                            return;
                        }
                        throw new AssertionError("state serialization exceeded its byte limit");
                    }
                });

        suite.reproduction("strict state export rejects entry count before serialization",
                new AllTests.CheckedRunnable() {
                    public void run() throws Exception {
                        try {
                            SwarmSessionManager.validateStateEntryCount(3, 2);
                        } catch (IllegalStateException expected) {
                            return;
                        }
                        throw new AssertionError("state export accepted excess entries");
                    }
                });

        suite.reproduction("initialized live payloads retain byte and entry bounds",
                new AllTests.CheckedRunnable() {
                    public void run() throws Exception {
                        expectPayloadRejected(new AllTests.CheckedRunnable() {
                            public void run() {
                                SwarmSessionManager.BootstrapBuffer.validatePayload(
                                        new byte[] {1, 2, 3}, 2, 3);
                            }
                        }, "live payload exceeded its configured byte limit");
                        expectPayloadRejected(new AllTests.CheckedRunnable() {
                            public void run() {
                                SwarmSessionManager.BootstrapBuffer.validatePayload(
                                        new SwarmSessionManager.SessionIdList(
                                                SwarmSessionManager.TOUCH,
                                                new Object[] {"one", "two"}), 1024, 1);
                            }
                        }, "live payload exceeded its configured entry limit");
                    }
                });

        suite.reproduction("sender rejects oversized session updates before transmission",
                new AllTests.CheckedRunnable() {
                    public void run() throws Exception {
                        final SwarmSessionManager.SessionUpdate update =
                                new SwarmSessionManager.SessionUpdate("session", 1L);
                        update.message = repeat('x', 128);
                        try {
                            SwarmSessionManager.validateSerializablePayload(update, 64);
                        } catch (IOException expected) {
                            return;
                        }
                        throw new AssertionError("oversized live update reached transmission");
                    }
                });

        suite.reproduction("sender chunks touch and discard ids before array allocation",
                new AllTests.CheckedRunnable() {
                    public void run() throws Exception {
                        Set ids = new LinkedHashSet();
                        ids.add("one");
                        ids.add("two");
                        ids.add("three");

                        SwarmSessionManager.SessionIdList first =
                                SwarmSessionManager.drainIds(
                                        SwarmSessionManager.TOUCH, ids, 4096, 2);
                        AllTests.assertEquals(Integer.valueOf(2),
                                Integer.valueOf(first.ids.length),
                                "sender did not honor the entry chunk limit");
                        AllTests.assertEquals(Integer.valueOf(1),
                                Integer.valueOf(ids.size()),
                                "sender removed ids outside the transmitted chunk");
                        SwarmSessionManager.SessionIdList second =
                                SwarmSessionManager.drainIds(
                                        SwarmSessionManager.TOUCH, ids, 4096, 2);
                        AllTests.assertEquals(Integer.valueOf(1),
                                Integer.valueOf(second.ids.length),
                                "sender did not retain the remaining id for the next chunk");
                        AllTests.assertTrue(ids.isEmpty(),
                                "sender retained an already transmitted id");
                    }
                });

        suite.reproduction("live replication limit failure demotes session readiness",
                new AllTests.CheckedRunnable() {
                    public void run() throws Exception {
                        TestManager manager = new TestManager();
                        SwarmLifecycle lifecycle = new SwarmLifecycle(
                                SwarmJoinPolicy.from(strictProperties()));
                        lifecycle.setCapability(
                                SwarmLifecycle.Capability.SESSION_READY);
                        manager.installInitializedLifecycle(lifecycle);

                        manager.recordLivePayloadFailure(
                                new IOException("oversized live payload"));

                        AllTests.assertTrue(!manager.isSessionStateInitialized(),
                                "live limit failure left session state initialized");
                        AllTests.assertEquals("ERROR",
                                manager.getSessionStateStatus(),
                                "live limit failure did not expose ERROR status");
                        AllTests.assertEquals(
                                SwarmLifecycle.Capability.SESSION_STARTING,
                                lifecycle.getCapability(),
                                "live limit failure left member as a state provider");
                        try {
                            manager.commitStrictState(new Hashtable(), 0, null);
                        } catch (IOException expected) {
                            AllTests.assertTrue(!manager.isSessionStateInitialized(),
                                    "invalidated bootstrap round committed after rejection");
                            return;
                        }
                        throw new AssertionError(
                                "invalidated bootstrap round committed dropped live state");
                    }
                });

        suite.reproduction("readiness publication is atomic with live failure demotion",
                new AllTests.CheckedRunnable() {
                    public void run() throws Exception {
                        SwarmLifecycle lifecycle = new SwarmLifecycle(
                                SwarmJoinPolicy.from(strictProperties()));
                        final BlockingReadyManager manager =
                                new BlockingReadyManager(lifecycle);
                        manager.installInitializedLifecycle(lifecycle);
                        final Throwable[] commitFailure = new Throwable[1];
                        Thread commit = new Thread(new Runnable() {
                            public void run() {
                                try {
                                    manager.commitStrictState(
                                            new Hashtable(), 0, null);
                                } catch (Throwable failure) {
                                    commitFailure[0] = failure;
                                }
                            }
                        }, "strict-state-commit-test");
                        commit.start();
                        AllTests.assertTrue(manager.readyEntered.await(
                                        2L, TimeUnit.SECONDS),
                                "state commit did not enter readiness publication");

                        Thread rejection = new Thread(new Runnable() {
                            public void run() {
                                manager.recordLivePayloadFailure(
                                        new IOException("concurrent rejection"));
                            }
                        }, "strict-live-rejection-test");
                        rejection.start();
                        manager.releaseReady.countDown();
                        commit.join(2000L);
                        rejection.join(2000L);

                        AllTests.assertTrue(!commit.isAlive() && !rejection.isAlive(),
                                "atomic readiness test threads did not finish");
                        AllTests.assertTrue(commitFailure[0] == null,
                                "state commit failed unexpectedly");
                        AllTests.assertTrue(!manager.isSessionStateInitialized(),
                                "concurrent rejection was overwritten by readiness");
                        AllTests.assertEquals("ERROR",
                                manager.getSessionStateStatus(),
                                "concurrent rejection did not remain visible");
                        AllTests.assertEquals(
                                SwarmLifecycle.Capability.SESSION_STARTING,
                                lifecycle.getCapability(),
                                "concurrent rejection was republished SESSION_READY");
                    }
                });

        suite.baseline("successful strict commit stays ready after a later singleton view",
                new AllTests.CheckedRunnable() {
                    public void run() throws Exception {
                        MutableViewChannel channel = new MutableViewChannel(1L, 2);
                        SwarmLifecycle lifecycle = new SwarmLifecycle(
                                SwarmJoinPolicy.from(strictProperties()));
                        lifecycle.publishPreReady(
                                new PullPushAdapter(channel, null, null, false));
                        ReadyLifecycleManager manager =
                                new ReadyLifecycleManager(lifecycle);
                        manager.installBootstrapLifecycle(lifecycle);
                        manager.runner = Thread.currentThread();
                        try {
                            manager.commitStrictState(new Hashtable(), 0, null);
                            channel.setView(2L, 1);

                            AllTests.assertTrue(manager.isSessionStateInitialized(),
                                    "later singleton view reset initialized session state");
                            AllTests.assertEquals("INITIALIZED",
                                    manager.getSessionStateStatus(),
                                    "later singleton view changed session state status");
                            AllTests.assertEquals(SwarmLifecycle.JoinStatus.READY,
                                    lifecycle.getJoinStatus(),
                                    "later singleton view demoted lifecycle readiness");
                        } finally {
                            manager.runner = null;
                        }
                    }
                });

        suite.reproduction("persistent bootstrap reuses Helma's held scripting engine",
                new AllTests.CheckedRunnable() {
                    public void run() throws Exception {
                        final TestManager manager = new TestManager();
                        manager.outer = new Hashtable();
                        manager.outer.put("remote", new byte[] {1});
                        manager.decoded = session("remote", manager, 1L);

                        final Object[] serialized = new Object[1];
                        ScriptingEngineInterface engine = (ScriptingEngineInterface)
                                Proxy.newProxyInstance(
                                        ScriptingEngineInterface.class.getClassLoader(),
                                        new Class[] {ScriptingEngineInterface.class},
                                        new InvocationHandler() {
                                            public Object invoke(Object proxy, Method method,
                                                                 Object[] args)
                                                    throws Exception {
                                                if ("serialize".equals(method.getName())) {
                                                    serialized[0] = args[0];
                                                    ((OutputStream) args[1]).write(1);
                                                    return null;
                                                }
                                                if ("deserialize".equals(method.getName())) {
                                                    int marker = ((InputStream) args[0]).read();
                                                    if (marker == 0) {
                                                        return manager.outer;
                                                    }
                                                    if (serialized[0] == null) {
                                                        return manager.decoded;
                                                    }
                                                    SwarmSession source =
                                                            (SwarmSession) serialized[0];
                                                    return session(source.getSessionId(), manager,
                                                            source.lastModified());
                                                }
                                                return null;
                                            }
                                        });

                        Hashtable imported = manager.importState(new byte[] {0}, engine);
                        AllTests.assertTrue(imported.containsKey("remote"),
                                "provider state was not decoded with the held engine");

                        SwarmSession disk = session("disk", manager, 2L);
                        manager.putSession("disk", disk);
                        Hashtable copied = copyCurrentSessions(manager, engine,
                                Integer.MAX_VALUE, Integer.MAX_VALUE);
                        AllTests.assertTrue(copied.containsKey("disk"),
                                "cold seed was not copied with the held engine");
                        AllTests.assertTrue(copied.get("disk") != disk,
                                "cold seed retained the live session instance");
                    }
                });

        suite.reproduction("persistent cold seed enforces state limits before readiness",
                new AllTests.CheckedRunnable() {
                    public void run() throws Exception {
                        final TestManager manager = new TestManager();
                        manager.putSession("one", session("one", manager, 1L));
                        manager.putSession("two", session("two", manager, 2L));
                        final Object[] serialized = new Object[1];
                        ScriptingEngineInterface engine = (ScriptingEngineInterface)
                                Proxy.newProxyInstance(
                                        ScriptingEngineInterface.class.getClassLoader(),
                                        new Class[] {ScriptingEngineInterface.class},
                                        new InvocationHandler() {
                                            public Object invoke(Object proxy, Method method,
                                                                 Object[] args)
                                                    throws Exception {
                                                if ("serialize".equals(method.getName())) {
                                                    serialized[0] = args[0];
                                                    OutputStream output = (OutputStream) args[1];
                                                    for (int i = 0; i < 8; i++) {
                                                        output.write(i);
                                                    }
                                                    return null;
                                                }
                                                if ("deserialize".equals(method.getName())) {
                                                    SwarmSession source =
                                                            (SwarmSession) serialized[0];
                                                    return session(source.getSessionId(), manager,
                                                            source.lastModified());
                                                }
                                                return null;
                                            }
                                        });

                        expectColdSeedRejected(manager, engine, 1024, 1,
                                "cold seed exceeded its entry limit");
                        expectColdSeedRejected(manager, engine, 4, 10,
                                "cold seed exceeded its byte limit");
                    }
                });

        suite.reproduction("strict state import is atomic when entry N is malformed",
                new AllTests.CheckedRunnable() {
                    public void run() throws Exception {
                        TestManager manager = new TestManager();
                        Hashtable encoded = new Hashtable();
                        encoded.put("good", new byte[] {1});
                        encoded.put("bad", new byte[] {2});
                        manager.outer = encoded;
                        manager.decoded = session("good", manager, 1L);
                        manager.failMarker = 2;
                        manager.putSession("existing", session("existing", manager, 3L));

                        try {
                            manager.importState(new byte[] {0});
                        } catch (IOException expected) {
                            AllTests.assertTrue(manager.containsSession("existing"),
                                    "failed import mutated the live session table");
                            AllTests.assertTrue(!manager.containsSession("good"),
                                    "partially decoded entry leaked into the live table");
                            return;
                        }
                        throw new AssertionError("malformed entry completed import");
                    }
                });

        suite.reproduction("buffer replay wins snapshot race by lastModified",
                new AllTests.CheckedRunnable() {
                    public void run() throws Exception {
                        TestManager manager = new TestManager();
                        Hashtable temporary = new Hashtable();
                        temporary.put("race", session("race", manager, 10L));
                        manager.decoded = session("race", manager, 20L);

                        manager.applyPayload(new byte[] {1}, temporary);
                        Session winner = (Session) temporary.get("race");
                        AllTests.assertEquals(Long.valueOf(20L),
                                Long.valueOf(winner.lastModified()),
                                "older snapshot overwrote newer buffered update");
                    }
                });

        suite.reproduction("cold seed preserves disk but authoritative provider replaces it",
                new AllTests.CheckedRunnable() {
                    public void run() {
                        Hashtable disk = new Hashtable();
                        disk.put("disk", "stale");
                        Hashtable provider = new Hashtable();
                        provider.put("remote", "authoritative");

                        Hashtable seed = SwarmSessionManager.selectInitialState(true, disk,
                                provider);
                        Hashtable joiner = SwarmSessionManager.selectInitialState(false, disk,
                                provider);
                        AllTests.assertTrue(seed.containsKey("disk")
                                        && !seed.containsKey("remote"),
                                "cold seed discarded persistent state");
                        AllTests.assertTrue(joiner.containsKey("remote")
                                        && !joiner.containsKey("disk"),
                                "joiner retained stale persistent state");
                    }
                });

        suite.reproduction("strict session initialization retries ordinary interrupts",
                new AllTests.CheckedRunnable() {
                    public void run() throws Exception {
                        String source = AllTests.source(
                                "src/helma/swarm/SwarmSessionManager.java");
                        String bootstrap = AllTests.methodBody(source,
                                "private void bootstrapStrictSessions(ScriptingEngineInterface loadEngine)");
                        require(bootstrap, "catch (InterruptedException");
                        require(bootstrap, "Thread.interrupted()");
                        require(bootstrap, "ProcessShutdown.current().isShuttingDown()");
                        require(bootstrap, "continue;");
                    }
                });

        suite.reproduction("strict session commit revalidates the bootstrap view",
                new AllTests.CheckedRunnable() {
                    public void run() throws Exception {
                        String source = AllTests.source(
                                "src/helma/swarm/SwarmSessionManager.java");
                        String bootstrap = AllTests.methodBody(source,
                                "private void bootstrapStrictSessions(ScriptingEngineInterface loadEngine)");
                        require(bootstrap,
                                "controlService.isCurrentBootstrapViewSufficient(view)");
                        require(bootstrap,
                                "initial session-state commit rejected");
                    }
                });

        suite.reproduction("non-session manager commits synchronously and cleans both layers",
                new AllTests.CheckedRunnable() {
                    public void run() throws Exception {
                        String source = AllTests.source(
                                "src/helma/swarm/SwarmNonSessionManager.java");
                        String init = AllTests.methodBody(source,
                                "public void init(Application app)");
                        require(init, "super.init(app)");
                        require(init, "ChannelUtils.getAdapter(app)");
                        require(init, "ChannelUtils.commitNonSessionReady(app, this)");
                        String shutdown = AllTests.methodBody(source,
                                "public void shutdown()");
                        require(shutdown, "super.shutdown()");
                        require(shutdown, "ChannelUtils.stopAdapter(app)");
                    }
                });
    }

    private static SwarmSession session(String id, SwarmSessionManager manager,
                                        long modified) {
        SwarmSession session = new SwarmSession(id, null, manager);
        session.setLastModified(modified);
        return session;
    }

    private static void require(String source, String token) {
        AllTests.assertTrue(source.contains(token), "missing required behavior: " + token);
    }

    private static void expectBufferOverflow(AllTests.CheckedRunnable operation,
                                             String message) throws Exception {
        try {
            operation.run();
        } catch (IllegalStateException expected) {
            return;
        }
        throw new AssertionError(message);
    }

    private static void expectPayloadRejected(AllTests.CheckedRunnable operation,
                                              String message) throws Exception {
        try {
            operation.run();
        } catch (IllegalArgumentException expected) {
            return;
        }
        throw new AssertionError(message);
    }

    private static void expectColdSeedRejected(
            SwarmSessionManager manager, ScriptingEngineInterface engine,
            int maxBytes, int maxEntries, String message) throws Exception {
        try {
            copyCurrentSessions(manager, engine, maxBytes, maxEntries);
        } catch (IOException expected) {
            return;
        }
        throw new AssertionError(message);
    }

    private static Hashtable copyCurrentSessions(
            SwarmSessionManager manager, ScriptingEngineInterface engine,
            int maxBytes, int maxEntries) throws Exception {
        Method copyCurrent = SwarmSessionManager.class.getDeclaredMethod(
                "copyCurrentSessions", new Class[] {
                        ScriptingEngineInterface.class, Integer.TYPE, Integer.TYPE});
        copyCurrent.setAccessible(true);
        try {
            return (Hashtable) copyCurrent.invoke(manager, new Object[] {
                    engine, Integer.valueOf(maxBytes), Integer.valueOf(maxEntries)});
        } catch (InvocationTargetException wrapped) {
            Throwable cause = wrapped.getTargetException();
            if (cause instanceof Exception) {
                throw (Exception) cause;
            }
            if (cause instanceof Error) {
                throw (Error) cause;
            }
            throw new RuntimeException(cause);
        }
    }

    private static String repeat(char value, int count) {
        StringBuffer result = new StringBuffer(count);
        for (int i = 0; i < count; i++) {
            result.append(value);
        }
        return result.toString();
    }

    private static java.util.Properties strictProperties() {
        java.util.Properties properties = new java.util.Properties();
        properties.setProperty("swarm.join.strict", "true");
        properties.setProperty("swarm.session.stateProviderMode", "strict");
        properties.setProperty("swarm.session.memberRole", "session");
        properties.setProperty("sessionManagerImpl",
                "helma.swarm.SwarmSessionManager");
        return properties;
    }

    private static class TestManager extends SwarmSessionManager {
        Hashtable outer;
        SwarmSession decoded;
        int failMarker = -1;

        void putSession(String id, Session session) {
            sessions.put(id, session);
        }

        boolean containsSession(String id) {
            return sessions.containsKey(id);
        }

        void installInitializedLifecycle(SwarmLifecycle value) throws Exception {
            installBootstrapLifecycle(value);
            Field initialized = SwarmSessionManager.class.getDeclaredField(
                    "sessionStateInitialized");
            initialized.setAccessible(true);
            initialized.setBoolean(this, true);
        }

        void installBootstrapLifecycle(SwarmLifecycle value) throws Exception {
            Field lifecycleField = SwarmSessionManager.class.getDeclaredField("lifecycle");
            lifecycleField.setAccessible(true);
            lifecycleField.set(this, value);
            Field buffer = SwarmSessionManager.class.getDeclaredField("bootstrapBuffer");
            buffer.setAccessible(true);
            buffer.set(this, new BootstrapBuffer(10, 4096, 10));
        }

        Object bytesToObject(byte[] bytes) throws IOException, ClassNotFoundException {
            int marker = bytes[0];
            if (marker == 0) {
                return outer;
            }
            if (marker == failMarker) {
                throw new IOException("malformed test entry");
            }
            return decoded;
        }
    }

    private static final class BlockingReadyManager extends TestManager {
        final CountDownLatch readyEntered = new CountDownLatch(1);
        final CountDownLatch releaseReady = new CountDownLatch(1);
        private final SwarmLifecycle lifecycle;

        BlockingReadyManager(SwarmLifecycle lifecycle) {
            this.lifecycle = lifecycle;
        }

        void publishSessionReady() {
            lifecycle.setCapability(SwarmLifecycle.Capability.SESSION_READY);
            readyEntered.countDown();
            try {
                if (!releaseReady.await(2L, TimeUnit.SECONDS)) {
                    throw new AssertionError("readiness release timed out");
                }
            } catch (InterruptedException interrupted) {
                throw new AssertionError("readiness publication interrupted", interrupted);
            }
        }
    }

    private static final class ReadyLifecycleManager extends TestManager {
        private final SwarmLifecycle lifecycle;

        ReadyLifecycleManager(SwarmLifecycle lifecycle) {
            this.lifecycle = lifecycle;
        }

        void publishSessionReady() {
            lifecycle.setCapability(SwarmLifecycle.Capability.SESSION_READY);
            lifecycle.commitReady();
        }
    }

    private static final class MutableViewChannel extends JChannel {
        private final Address local = new UUID(0, 2000L);
        private View view;

        MutableViewChannel(long viewSequence, int size) {
            super(false);
            setView(viewSequence, size);
        }

        void setView(long viewSequence, int size) {
            ArrayList<Address> members = new ArrayList<Address>();
            members.add(local);
            for (int i = 1; i < size; i++) {
                members.add(new UUID(0, 2000L + i));
            }
            view = new View(local, viewSequence, members);
        }

        public View getView() {
            return view;
        }

        public Address getLocalAddress() {
            return local;
        }
    }
}
