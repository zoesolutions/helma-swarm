package helma.swarm;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.lang.reflect.Field;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import helma.framework.core.Application;
import helma.util.ResourceProperties;
import org.jgroups.Address;
import org.jgroups.Channel;
import org.jgroups.ChannelException;
import org.jgroups.JChannel;
import org.jgroups.View;
import org.jgroups.blocks.PullPushAdapter;
import org.jgroups.util.UUID;

final class SwarmChannelBootstrapRuntimeTest {

    private SwarmChannelBootstrapRuntimeTest() {
    }

    static void register(AllTests.Suite suite) {
        suite.reproduction("strict role rejects an incompatible session manager",
                new AllTests.CheckedRunnable() {
                    public void run() throws Exception {
                        final Properties session = strictProperties("session");
                        session.setProperty("sessionManagerImpl",
                                "helma.framework.core.SessionManager");
                        AllTests.expectRejected(new AllTests.CheckedRunnable() {
                            public void run() {
                                SwarmJoinPolicy.from(session);
                            }
                        }, "session role accepted an incompatible manager");

                        final Properties nonSession = strictProperties("non-session");
                        nonSession.setProperty("sessionManagerImpl",
                                "helma.swarm.SwarmSessionManager");
                        AllTests.expectRejected(new AllTests.CheckedRunnable() {
                            public void run() {
                                SwarmJoinPolicy.from(nonSession);
                            }
                        }, "non-session role accepted an incompatible manager");
                    }
                });

        suite.reproduction("strict preflight accepts only the fixed health query",
                new AllTests.CheckedRunnable() {
                    public void run() throws Exception {
                        final Properties dangerous = strictProperties("session");
                        dangerous.setProperty("swarm.join.validationQuery",
                                "DELETE FROM OnlineExam");
                        AllTests.expectRejected(new AllTests.CheckedRunnable() {
                            public void run() {
                                SwarmJoinPolicy.from(dangerous);
                            }
                        }, "STRICT preflight accepted an arbitrary validation query");

                        Properties safe = strictProperties("session");
                        safe.setProperty("swarm.join.validationQuery", "  select 1  ");
                        SwarmJoinPolicy policy = SwarmJoinPolicy.from(safe);
                        AllTests.assertEquals("select 1", policy.getValidationQuery(),
                                "fixed health query was not normalized at the boundary");
                    }
                });

        suite.reproduction("strict state and buffer limits must be positive",
                new AllTests.CheckedRunnable() {
                    public void run() throws Exception {
                        assertInvalidLimit("swarm.session.stateMaxBytes");
                        assertInvalidLimit("swarm.session.stateMaxEntries");
                        assertInvalidLimit("swarm.session.bootstrapBufferMaxBytes");
                        assertInvalidLimit("swarm.session.bootstrapBufferMaxEntries");
                    }
                });

        suite.reproduction("strict database query timeout must be positive",
                new AllTests.CheckedRunnable() {
                    public void run() throws Exception {
                        final Properties properties = strictProperties("session");
                        properties.setProperty(
                                "swarm.join.validationQueryTimeoutSeconds", "0");
                        AllTests.expectRejected(new AllTests.CheckedRunnable() {
                            public void run() {
                                SwarmJoinPolicy.from(properties);
                            }
                        }, "STRICT accepted an unbounded database query timeout");
                    }
                });

        suite.reproduction("bootstrap retains channel ownership until publication",
                new AllTests.CheckedRunnable() {
                    public void run() throws Exception {
                        Application app = strictApplication("ownership");
                        SwarmJoinPolicy policy = SwarmJoinPolicy.from(app.getProperties());
                        SwarmLifecycle lifecycle = new SwarmLifecycle(policy);
                        FakeChannel channel = new FakeChannel(2);
                        PullPushAdapter adapter = bootstrap(app, lifecycle,
                                new FixedChannelFactory(channel)).bootstrap();

                        AllTests.assertTrue(lifecycle.getChannel() == channel,
                                "successful bootstrap released its channel before publication");
                        lifecycle.stop();
                        AllTests.assertTrue(!channel.isOpen(),
                                "shutdown between bootstrap and publication leaked the channel");
                        AllTests.assertTrue(!lifecycle.publishPreReady(adapter),
                                "stopped lifecycle accepted PRE_READY publication");
                        adapter.stop();
                    }
                });

        suite.reproduction("bootstrap retries with a fresh channel and closes the singleton",
                new AllTests.CheckedRunnable() {
                    public void run() throws Exception {
                        Application app = strictApplication("retry");
                        SwarmJoinPolicy policy = SwarmJoinPolicy.from(app.getProperties());
                        SwarmLifecycle lifecycle = new SwarmLifecycle(policy);
                        FakeChannel singleton = new FakeChannel(1);
                        FakeChannel recovered = new FakeChannel(2);
                        SequenceChannelFactory channels =
                                new SequenceChannelFactory(singleton, recovered);

                        PullPushAdapter adapter = bootstrap(app, lifecycle, channels).bootstrap();

                        AllTests.assertEquals(Integer.valueOf(2),
                                Integer.valueOf(channels.created()),
                                "bootstrap did not create a fresh retry channel");
                        AllTests.assertTrue(!singleton.isOpen(),
                                "failed singleton channel remained open");
                        AllTests.assertTrue(recovered.isConnected(),
                                "fresh retry channel did not connect");
                        lifecycle.stop();
                        adapter.stop();
                    }
                });

        suite.reproduction("parallel callers share one unpublished bootstrap",
                new AllTests.CheckedRunnable() {
                    public void run() throws Exception {
                        final Application app = strictApplication("parallel");
                        final CountDownLatch entered = new CountDownLatch(1);
                        final CountDownLatch release = new CountDownLatch(1);
                        final AtomicInteger starts = new AtomicInteger();
                        final Object[] results = new Object[2];
                        final Throwable[] failures = new Throwable[2];
                        final FakeChannel channel = new FakeChannel(2);
                        final StrictChannelBootstrap bootstrap =
                                new StrictChannelBootstrap() {
                                    public PullPushAdapter bootstrap(
                                            Application target,
                                            SwarmLifecycle lifecycle) throws ChannelException {
                                        starts.incrementAndGet();
                                        entered.countDown();
                                        try {
                                            if (!release.await(2L, TimeUnit.SECONDS)) {
                                                throw new ChannelException(
                                                        "test bootstrap release timed out");
                                            }
                                            channel.connect("parallel-test");
                                            return new PullPushAdapter(
                                                    channel, null, null, false);
                                        } catch (InterruptedException interrupted) {
                                            throw new ChannelException(interrupted.toString());
                                        }
                                    }
                                };

                        Thread first = adapterCaller(app, bootstrap, results, failures, 0);
                        Thread second = adapterCaller(app, bootstrap, results, failures, 1);
                        first.start();
                        AllTests.assertTrue(entered.await(2L, TimeUnit.SECONDS),
                                "first bootstrap did not start");
                        AllTests.assertTrue(ChannelUtils.getExistingAdapter(app) == null,
                                "adapter was published before bootstrap completed");
                        second.start();
                        release.countDown();
                        first.join(2000L);
                        second.join(2000L);
                        try {
                            AllTests.assertTrue(!first.isAlive() && !second.isAlive(),
                                    "parallel callers did not complete");
                            AllTests.assertTrue(failures[0] == null && failures[1] == null,
                                    "parallel adapter call failed");
                            AllTests.assertTrue(results[0] == results[1],
                                    "parallel callers received different adapters");
                            AllTests.assertEquals(Integer.valueOf(1),
                                    Integer.valueOf(starts.get()),
                                    "parallel callers started more than one bootstrap");
                        } finally {
                            ChannelUtils.stopAdapter(app);
                        }
                    }
                });

        suite.reproduction("ordinary bootstrap interrupt retries with a fresh channel",
                new AllTests.CheckedRunnable() {
                    public void run() throws Exception {
                        Application app = strictApplication("interrupt");
                        app.getProperties().setProperty(
                                "swarm.join.minViewWaitMillis", "1000");
                        SwarmLifecycle lifecycle = new SwarmLifecycle(
                                SwarmJoinPolicy.from(app.getProperties()));
                        FakeChannel interrupted = new FakeChannel(1);
                        FakeChannel recovered = new FakeChannel(2);
                        SequenceChannelFactory channels =
                                new SequenceChannelFactory(interrupted, recovered);
                        Sleeper sleeper = new Sleeper() {
                            private boolean first = true;

                            public void sleep(long delayMillis)
                                    throws InterruptedException {
                                if (first) {
                                    first = false;
                                    Thread.currentThread().interrupt();
                                    throw new InterruptedException("test interrupt");
                                }
                            }
                        };

                        PullPushAdapter adapter = bootstrap(app, lifecycle, channels,
                                sleeper, new ProcessShutdown(false)).bootstrap();
                        AllTests.assertEquals(Integer.valueOf(2),
                                Integer.valueOf(channels.created()),
                                "interrupt escaped instead of starting a fresh attempt");
                        AllTests.assertTrue(!interrupted.isOpen(),
                                "interrupted candidate remained open");
                        AllTests.assertTrue(!Thread.currentThread().isInterrupted(),
                                "handled interrupt leaked to the caller");
                        lifecycle.stop();
                        adapter.stop();
                    }
                });

        suite.reproduction("process shutdown terminates bootstrap and closes candidate",
                new AllTests.CheckedRunnable() {
                    public void run() throws Exception {
                        Application app = strictApplication("shutdown");
                        app.getProperties().setProperty(
                                "swarm.join.minViewWaitMillis", "1000");
                        SwarmLifecycle lifecycle = new SwarmLifecycle(
                                SwarmJoinPolicy.from(app.getProperties()));
                        final ProcessShutdown shutdown = new ProcessShutdown(false);
                        FakeChannel candidate = new FakeChannel(1);
                        Sleeper sleeper = new Sleeper() {
                            public void sleep(long delayMillis)
                                    throws InterruptedException {
                                shutdown.signalShutdown();
                                throw new InterruptedException("test shutdown");
                            }
                        };

                        try {
                            bootstrap(app, lifecycle,
                                    new FixedChannelFactory(candidate), sleeper,
                                    shutdown).bootstrap();
                        } catch (ChannelException expected) {
                            AllTests.assertTrue(!candidate.isOpen(),
                                    "shutdown left the candidate open");
                            AllTests.assertEquals(SwarmLifecycle.JoinStatus.STOPPED,
                                    lifecycle.getJoinStatus(),
                                    "shutdown did not stop the lifecycle");
                            return;
                        }
                        throw new AssertionError("shutdown allowed bootstrap publication");
                    }
                });
    }

    private static Thread adapterCaller(final Application app,
                                        final StrictChannelBootstrap bootstrap,
                                        final Object[] results,
                                        final Throwable[] failures,
                                        final int index) {
        return new Thread(new Runnable() {
            public void run() {
                try {
                    results[index] = ChannelUtils.getAdapter(app, bootstrap);
                } catch (Throwable failure) {
                    failures[index] = failure;
                }
            }
        }, "swarm-adapter-test-" + index);
    }

    private static SwarmChannelBootstrap bootstrap(Application app,
                                                     SwarmLifecycle lifecycle,
                                                     ChannelFactory channels) {
        return new SwarmChannelBootstrap(app, lifecycle,
                new DatabaseProbe() {
                    public void validate(Application ignored, SwarmJoinPolicy policy) {
                    }
                }, channels, new Sleeper() {
                    public void sleep(long delayMillis) {
                    }
                }, new ProcessShutdown(false));
    }

    private static SwarmChannelBootstrap bootstrap(Application app,
                                                     SwarmLifecycle lifecycle,
                                                     ChannelFactory channels,
                                                     Sleeper sleeper,
                                                     ProcessShutdown shutdown) {
        return new SwarmChannelBootstrap(app, lifecycle,
                new DatabaseProbe() {
                    public void validate(Application ignored, SwarmJoinPolicy policy) {
                    }
                }, channels, sleeper, shutdown);
    }

    private static Application strictApplication(String name) {
        Application app = new Application("bootstrap-" + name);
        installProperties(app);
        app.getProperties().putAll(strictProperties("session"));
        app.getProperties().setProperty("swarm.name", "bootstrap-" + name);
        return app;
    }

    private static void installProperties(Application app) {
        try {
            Field field = Application.class.getDeclaredField("props");
            field.setAccessible(true);
            field.set(app, new ResourceProperties());
            Field eventLogName = Application.class.getDeclaredField("eventLogName");
            eventLogName.setAccessible(true);
            eventLogName.set(app, "bootstrap-runtime-test");
        } catch (Exception failure) {
            throw new AssertionError("could not initialize Application properties", failure);
        }
    }

    private static Properties strictProperties(String role) {
        Properties properties = new Properties();
        properties.setProperty("swarm.join.strict", "true");
        properties.setProperty("swarm.join.retryInitialDelayMillis", "0");
        properties.setProperty("swarm.join.retryMaxDelayMillis", "0");
        properties.setProperty("swarm.join.maxAttempts", "0");
        properties.setProperty("swarm.join.minViewSize", "2");
        properties.setProperty("swarm.join.minViewWaitMillis", "0");
        properties.setProperty("swarm.session.stateProviderMode", "strict");
        properties.setProperty("swarm.session.memberRole", role);
        properties.setProperty("sessionManagerImpl", "session".equals(role)
                ? "helma.swarm.SwarmSessionManager"
                : "helma.swarm.SwarmNonSessionManager");
        return properties;
    }

    private static void assertInvalidLimit(final String name) throws Exception {
        final Properties properties = strictProperties("session");
        properties.setProperty(name, "0");
        AllTests.expectRejected(new AllTests.CheckedRunnable() {
            public void run() {
                SwarmJoinPolicy.from(properties);
            }
        }, name + " accepted zero");
    }

    private static final class FixedChannelFactory implements ChannelFactory {
        private final Channel channel;

        FixedChannelFactory(Channel channel) {
            this.channel = channel;
        }

        public Channel create(Application app) {
            return channel;
        }
    }

    private static final class SequenceChannelFactory implements ChannelFactory {
        private final List channels = new ArrayList();
        private int offset;

        SequenceChannelFactory(Channel first, Channel second) {
            channels.add(first);
            channels.add(second);
        }

        public Channel create(Application app) {
            if (offset >= channels.size()) {
                throw new AssertionError("unexpected bootstrap attempt");
            }
            return (Channel) channels.get(offset++);
        }

        int created() {
            return offset;
        }
    }

    private static final class FakeChannel extends JChannel {
        private final Address local = new UUID(0, System.identityHashCode(this));
        private final View view;
        private boolean open = true;
        private boolean connected;

        FakeChannel(int viewSize) {
            super(false);
            List members = new ArrayList();
            members.add(local);
            if (viewSize > 1) {
                members.add(new UUID(0, System.identityHashCode(this) + 1));
            }
            view = new View(local, 1L, members);
        }

        public synchronized void connect(String clusterName) {
            connected = true;
        }

        public synchronized void disconnect() {
            connected = false;
        }

        public synchronized void close() {
            connected = false;
            open = false;
        }

        public boolean isOpen() {
            return open;
        }

        public boolean isConnected() {
            return connected;
        }

        public View getView() {
            return view;
        }

        public Address getLocalAddress() {
            return local;
        }
    }
}
