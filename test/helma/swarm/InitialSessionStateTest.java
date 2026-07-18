package helma.swarm;

import helma.framework.core.Application;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.jgroups.ChannelException;
import org.jgroups.View;
import org.jgroups.stack.IpAddress;

final class InitialSessionStateTest {

    private InitialSessionStateTest() {
    }

    static void register(AllTests.Suite suite) {
        suite.reproduction("empty decoded session state acknowledges initial apply",
                new AllTests.CheckedRunnable() {
                    public void run() {
                        StubManager manager = manager();
                        manager.decoded.add(new Hashtable());
                        manager.initialStateAttemptActive = true;
                        manager.setState(new byte[] {1});
                        AllTests.assertTrue(manager.initialStateApplied,
                                "empty valid state was not acknowledged");
                    }
                });
        suite.reproduction("outer session state decode failure is not acknowledged",
                new AllTests.CheckedRunnable() {
                    public void run() {
                        StubManager manager = manager();
                        manager.failure = new IOException("simulated outer decode failure");
                        manager.initialStateAttemptActive = true;
                        manager.setState(new byte[] {1});
                        AllTests.assertTrue(!manager.initialStateApplied,
                                "outer decode failure was acknowledged");
                    }
                });
        suite.reproduction("complete decoded session state is applied and acknowledged",
                new AllTests.CheckedRunnable() {
                    public void run() {
                        StubManager manager = manager();
                        Application app = new Application("session-state-test");
                        manager.initialize(app);
                        SwarmSession session = new SwarmSession(
                                "replicated-session", app, manager);
                        Hashtable state = new Hashtable();
                        state.put(session.getSessionId(), session);
                        manager.decoded.add(state);
                        manager.initialStateAttemptActive = true;
                        manager.setState(new byte[] {1});
                        AllTests.assertTrue(manager.initialStateApplied,
                                "complete state was not acknowledged");
                        AllTests.assertTrue(manager.getSession(
                                "replicated-session") == session,
                                "complete state was not registered");
                    }
                });
        suite.reproduction("per-entry session state failure is not acknowledged",
                new AllTests.CheckedRunnable() {
                    public void run() {
                        StubManager manager = manager();
                        Hashtable state = new Hashtable();
                        state.put("broken", new Object());
                        manager.decoded.add(state);
                        manager.initialStateAttemptActive = true;
                        manager.setState(new byte[] {1});
                        AllTests.assertTrue(!manager.initialStateApplied,
                                "per-entry decode failure was acknowledged");
                    }
                });
        suite.reproduction("failed session state does not partially mutate live sessions",
                new AllTests.CheckedRunnable() {
                    public void run() {
                        StubManager manager = manager();
                        Application app = new Application("atomic-session-state-test");
                        manager.initialize(app);
                        SwarmSession valid = new SwarmSession(
                                "must-not-leak", app, manager);
                        OrderedHashtable state = new OrderedHashtable();
                        state.put(valid.getSessionId(), valid);
                        state.put("broken", new Object());
                        manager.decoded.add(state);
                        manager.initialStateAttemptActive = true;

                        manager.setState(new byte[] {1});

                        AllTests.assertTrue(!manager.initialStateApplied,
                                "mixed valid/invalid state was acknowledged");
                        AllTests.assertTrue(manager.getSession("must-not-leak") == null,
                                "failed state partially registered a session");
                    }
                });
        suite.reproduction("legacy state transfer still applies valid entries independently",
                new AllTests.CheckedRunnable() {
                    public void run() {
                        StubManager manager = manager();
                        Application app = new Application("legacy-session-state-test");
                        manager.initialize(app);
                        SwarmSession valid = new SwarmSession(
                                "legacy-valid", app, manager);
                        OrderedHashtable state = new OrderedHashtable();
                        state.put(valid.getSessionId(), valid);
                        state.put("broken", new Object());
                        manager.decoded.add(state);

                        manager.setState(new byte[] {1});

                        AllTests.assertTrue(manager.getSession("legacy-valid") == valid,
                                "legacy transfer discarded a valid entry");
                    }
                });
        suite.reproduction("initial session state retries until transfer is applied",
                new AllTests.CheckedRunnable() {
                    public void run() throws Exception {
                        final FakeAck ack = new FakeAck();
                        final int[] attempts = new int[1];
                        final List caps = new ArrayList();
                        InitialSessionStateSynchronizer synchronizer = synchronizer(
                                new InitialSessionStateSynchronizer.Transfer() {
                                    public boolean request() throws Exception {
                                        int attempt = attempts[0]++;
                                        if (attempt == 0) {
                                            return false;
                                        }
                                        if (attempt == 1) {
                                            throw new IOException("transient state failure");
                                        }
                                        if (attempt == 3) {
                                            ack.applied = true;
                                        }
                                        return true;
                                    }
                                }, ack,
                                new InitialSessionStateSynchronizer.SeedPolicy() {
                                    public boolean maySeed() {
                                        return false;
                                    }
                                }, new InitialSessionStateSynchronizer.Delay() {
                                    public long next(long capMillis) {
                                        caps.add(Long.valueOf(capMillis));
                                        return 1L;
                                    }
                                }, new SessionStateStartupToken());
                        synchronizer.synchronize();

                        AllTests.assertEquals(Integer.valueOf(4),
                                Integer.valueOf(attempts[0]),
                                "session state retry stopped before apply acknowledgement");
                        AllTests.assertEquals(Long.valueOf(1000L), caps.get(0),
                                "initial state retry cap changed");
                        AllTests.assertEquals(Long.valueOf(2000L), caps.get(1),
                                "second state retry cap changed");
                        AllTests.assertEquals(Long.valueOf(4000L), caps.get(2),
                                "third state retry cap changed");
                    }
                });
        suite.reproduction("true state transfer waits for its delayed apply callback",
                new AllTests.CheckedRunnable() {
                    public void run() throws Exception {
                        final AtomicInteger attempts = new AtomicInteger();
                        final CountDownLatch awaitingApply = new CountDownLatch(1);
                        final CountDownLatch applied = new CountDownLatch(1);
                        final AtomicReference failure = new AtomicReference();
                        final InitialSessionStateSynchronizer.ApplyAck ack =
                                new InitialSessionStateSynchronizer.ApplyAck() {
                                    volatile boolean complete;

                                    public void begin() {
                                        complete = false;
                                    }

                                    public boolean isApplied() {
                                        return complete;
                                    }

                                    public boolean awaitApplied(long timeoutMillis)
                                            throws InterruptedException {
                                        awaitingApply.countDown();
                                        applied.await();
                                        complete = true;
                                        return true;
                                    }

                                    public void end() {
                                    }
                                };
                        final InitialSessionStateSynchronizer synchronizer = synchronizer(
                                new InitialSessionStateSynchronizer.Transfer() {
                                    public boolean request() {
                                        attempts.incrementAndGet();
                                        return true;
                                    }
                                }, ack,
                                new InitialSessionStateSynchronizer.SeedPolicy() {
                                    public boolean maySeed() {
                                        return false;
                                    }
                                }, new InitialSessionStateSynchronizer.Delay() {
                                    public long next(long capMillis) {
                                        throw new AssertionError(
                                                "pending apply entered retry backoff");
                                    }
                                }, new SessionStateStartupToken());
                        Thread worker = new Thread(new Runnable() {
                            public void run() {
                                try {
                                    synchronizer.synchronize();
                                } catch (Throwable problem) {
                                    failure.set(problem);
                                }
                            }
                        }, "delayed-session-apply");
                        worker.start();
                        AllTests.assertTrue(awaitingApply.await(3000L,
                                        TimeUnit.MILLISECONDS),
                                "state transfer did not await apply callback");
                        Thread.sleep(100L);
                        AllTests.assertEquals(Integer.valueOf(1),
                                Integer.valueOf(attempts.get()),
                                "pending callback started a second transfer");
                        AllTests.assertTrue(worker.isAlive(),
                                "startup continued before apply callback");
                        applied.countDown();
                        worker.join(3000L);
                        AllTests.assertTrue(!worker.isAlive(),
                                "applied state did not release startup");
                        AllTests.assertTrue(failure.get() == null,
                                "delayed apply failed: " + failure.get());
                    }
                });
        suite.reproduction("initial coordinator may cold-seed after failed transfer",
                new AllTests.CheckedRunnable() {
                    public void run() throws Exception {
                        final int[] attempts = new int[1];
                        InitialSessionStateSynchronizer synchronizer = synchronizer(
                                new InitialSessionStateSynchronizer.Transfer() {
                                    public boolean request() {
                                        attempts[0]++;
                                        return false;
                                    }
                                }, new FakeAck(),
                                new InitialSessionStateSynchronizer.SeedPolicy() {
                                    public boolean maySeed() {
                                        return true;
                                    }
                                }, new InitialSessionStateSynchronizer.Delay() {
                                    public long next(long capMillis) {
                                        throw new AssertionError(
                                                "cold seed entered retry backoff");
                                    }
                                }, new SessionStateStartupToken());
                        synchronizer.synchronize();
                        AllTests.assertEquals(Integer.valueOf(1),
                                Integer.valueOf(attempts[0]),
                                "cold seed skipped or repeated initial transfer");
                    }
                });
        suite.reproduction("noncoordinator cannot become cold seed after view change",
                new AllTests.CheckedRunnable() {
                    public void run() {
                        IpAddress first = new IpAddress(17820);
                        IpAddress local = new IpAddress(17821);
                        java.util.Vector initialMembers = new java.util.Vector();
                        initialMembers.add(first);
                        initialMembers.add(local);
                        View initialView = new View(first, 1L, initialMembers);
                        boolean initialCoordinator =
                                SwarmSessionManager.isInitialCoordinator(
                                        local, initialView);

                        java.util.Vector laterMembers = new java.util.Vector();
                        laterMembers.add(local);
                        View laterView = new View(local, 2L, laterMembers);
                        AllTests.assertTrue(
                                SwarmSessionManager.isInitialCoordinator(
                                        local, laterView),
                                "test did not model later coordinator role");
                        AllTests.assertTrue(!initialCoordinator,
                                "later view retroactively changed seed eligibility");
                    }
                });
        suite.reproduction("noncoordinator remains blocked after becoming coordinator",
                new AllTests.CheckedRunnable() {
                    public void run() throws Exception {
                        final SessionStateStartupToken token =
                                new SessionStateStartupToken();
                        final AtomicInteger attempts = new AtomicInteger();
                        final boolean initialCoordinator = false;
                        InitialSessionStateSynchronizer synchronizer = synchronizer(
                                new InitialSessionStateSynchronizer.Transfer() {
                                    public boolean request() {
                                        attempts.incrementAndGet();
                                        return false;
                                    }
                                }, new FakeAck(),
                                new InitialSessionStateSynchronizer.SeedPolicy() {
                                    public boolean maySeed() {
                                        return initialCoordinator;
                                    }
                                }, new InitialSessionStateSynchronizer.Delay() {
                                    public long next(long capMillis) {
                                        token.cancel();
                                        return capMillis;
                                    }
                                }, token);
                        try {
                            synchronizer.synchronize();
                            throw new AssertionError(
                                    "later coordinator role enabled cold seed");
                        } catch (ChannelException expected) {
                            AllTests.assertEquals(Integer.valueOf(1),
                                    Integer.valueOf(attempts.get()),
                                    "noncoordinator continued after cancellation");
                        }
                    }
                });
        suite.reproduction("session state retry cancellation exits without busy loop",
                new AllTests.CheckedRunnable() {
                    public void run() throws Exception {
                        final SessionStateStartupToken token =
                                new SessionStateStartupToken();
                        final int[] attempts = new int[1];
                        InitialSessionStateSynchronizer synchronizer = synchronizer(
                                new InitialSessionStateSynchronizer.Transfer() {
                                    public boolean request() {
                                        attempts[0]++;
                                        return false;
                                    }
                                }, new FakeAck(),
                                new InitialSessionStateSynchronizer.SeedPolicy() {
                                    public boolean maySeed() {
                                        return false;
                                    }
                                }, new InitialSessionStateSynchronizer.Delay() {
                                    public long next(long capMillis) {
                                        token.cancel();
                                        return capMillis;
                                    }
                                }, token);
                        try {
                            synchronizer.synchronize();
                            throw new AssertionError(
                                    "cancelled session state retry returned success");
                        } catch (ChannelException expected) {
                            AllTests.assertEquals(Integer.valueOf(1),
                                    Integer.valueOf(attempts[0]),
                                    "cancelled session state retry kept polling");
                        }
                    }
                });
        suite.reproduction("session cancellation cannot fall through to cold seed",
                new AllTests.CheckedRunnable() {
                    public void run() throws Exception {
                        final SessionStateStartupToken token =
                                new SessionStateStartupToken();
                        InitialSessionStateSynchronizer synchronizer = synchronizer(
                                new InitialSessionStateSynchronizer.Transfer() {
                                    public boolean request() {
                                        token.cancel();
                                        return false;
                                    }
                                }, new FakeAck(),
                                new InitialSessionStateSynchronizer.SeedPolicy() {
                                    public boolean maySeed() {
                                        return true;
                                    }
                                }, new InitialSessionStateSynchronizer.Delay() {
                                    public long next(long capMillis) {
                                        throw new AssertionError(
                                                "cancelled transfer entered backoff");
                                    }
                                }, token);
                        try {
                            synchronizer.synchronize();
                            throw new AssertionError(
                                    "cancelled transfer was accepted as cold seed");
                        } catch (ChannelException expected) {
                        }
                    }
                });
        suite.reproduction("unexpected session backoff interrupt fail-stops until cancellation",
                new AllTests.CheckedRunnable() {
                    public void run() throws Exception {
                        final SessionStateStartupToken token =
                                new SessionStateStartupToken();
                        final AtomicInteger attempts = new AtomicInteger();
                        final CountDownLatch inBackoff = new CountDownLatch(1);
                        final AtomicReference failure = new AtomicReference();
                        final AtomicReference interruptRestored = new AtomicReference();
                        final InitialSessionStateSynchronizer synchronizer = synchronizer(
                                new InitialSessionStateSynchronizer.Transfer() {
                                    public boolean request() {
                                        attempts.incrementAndGet();
                                        return false;
                                    }
                                }, new FakeAck(),
                                new InitialSessionStateSynchronizer.SeedPolicy() {
                                    public boolean maySeed() {
                                        return false;
                                    }
                                }, new InitialSessionStateSynchronizer.Delay() {
                                    public long next(long capMillis) {
                                        inBackoff.countDown();
                                        return 60000L;
                                    }
                                }, token);
                        Thread worker = new Thread(new Runnable() {
                            public void run() {
                                try {
                                    synchronizer.synchronize();
                                } catch (Throwable problem) {
                                    failure.set(problem);
                                    interruptRestored.set(Boolean.valueOf(
                                            Thread.currentThread().isInterrupted()));
                                }
                            }
                        }, "session-unexpected-interrupt");
                        worker.start();
                        AllTests.assertTrue(inBackoff.await(3000L, TimeUnit.MILLISECONDS),
                                "session retry did not enter backoff");
                        worker.interrupt();
                        Thread.sleep(100L);
                        AllTests.assertEquals(Integer.valueOf(1),
                                Integer.valueOf(attempts.get()),
                                "unexpected interrupt started another transfer");
                        AllTests.assertTrue(worker.isAlive(),
                                "unexpected interrupt returned from fail-stop");
                        token.cancel();
                        worker.join(3000L);
                        AllTests.assertTrue(!worker.isAlive(),
                                "cancelled fail-stop did not finish");
                        AllTests.assertTrue(failure.get() instanceof ChannelException,
                                "fail-stop did not end as a channel failure");
                        AllTests.assertEquals(Boolean.TRUE, interruptRestored.get(),
                                "unexpected interrupt flag was not restored");
                    }
                });
    }

    private static StubManager manager() {
        StubManager manager = new StubManager();
        manager.log = LogFactory.getLog(InitialSessionStateTest.class);
        return manager;
    }

    private static InitialSessionStateSynchronizer synchronizer(
            InitialSessionStateSynchronizer.Transfer transfer,
            InitialSessionStateSynchronizer.ApplyAck ack,
            InitialSessionStateSynchronizer.SeedPolicy seed,
            InitialSessionStateSynchronizer.Delay delay,
            SessionStateStartupToken token) {
        return new InitialSessionStateSynchronizer(
                transfer, ack, seed, delay, token);
    }

    private static final class FakeAck
            implements InitialSessionStateSynchronizer.ApplyAck {
        boolean applied;

        public void begin() {
            applied = false;
        }

        public boolean isApplied() {
            return applied;
        }

        public boolean awaitApplied(long timeoutMillis) {
            return applied;
        }

        public void end() {
        }
    }

    private static final class StubManager extends SwarmSessionManager {
        final List decoded = new ArrayList();
        IOException failure;

        void initialize(Application application) {
            this.app = application;
        }

        Object bytesToObject(byte[] bytes) throws IOException {
            if (failure != null) {
                throw failure;
            }
            return decoded.remove(0);
        }
    }

    private static final class OrderedHashtable extends Hashtable {
        private final Set orderedEntries = new LinkedHashSet();

        public synchronized Object put(Object key, Object value) {
            Object previous = super.put(key, value);
            orderedEntries.remove(new OrderedEntry(key, previous));
            orderedEntries.add(new OrderedEntry(key, value));
            return previous;
        }

        public Set entrySet() {
            return orderedEntries;
        }
    }

    private static final class OrderedEntry implements Map.Entry {
        private final Object key;
        private Object value;

        OrderedEntry(Object key, Object value) {
            this.key = key;
            this.value = value;
        }

        public Object getKey() {
            return key;
        }

        public Object getValue() {
            return value;
        }

        public Object setValue(Object replacement) {
            Object previous = value;
            value = replacement;
            return previous;
        }

        public boolean equals(Object other) {
            return other instanceof Map.Entry
                    && equal(key, ((Map.Entry) other).getKey());
        }

        public int hashCode() {
            return key == null ? 0 : key.hashCode();
        }

        private static boolean equal(Object left, Object right) {
            return left == null ? right == null : left.equals(right);
        }
    }
}
