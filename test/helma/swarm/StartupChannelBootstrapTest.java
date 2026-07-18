package helma.swarm;

import helma.framework.core.Application;
import helma.util.ResourceProperties;
import org.apache.commons.logging.LogFactory;
import org.jgroups.Channel;
import org.jgroups.ChannelException;
import org.jgroups.JChannel;
import org.jgroups.View;
import org.jgroups.blocks.PullPushAdapter;
import org.jgroups.stack.IpAddress;

import java.util.ArrayList;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.lang.reflect.Field;

final class StartupChannelBootstrapTest {

    private StartupChannelBootstrapTest() {
    }

    static void register(AllTests.Suite suite) {
        suite.reproduction("startup-only policy accepts the documented profile",
                new AllTests.CheckedRunnable() {
                    public void run() {
                        java.util.Properties properties = new java.util.Properties();
                        properties.setProperty("swarm.join.startupRetry", "true");
                        properties.setProperty("swarm.join.minViewSize", "2");
                        properties.setProperty("swarm.join.minViewWaitMillis", "10000");
                        properties.setProperty("swarm.join.retryInitialDelayMillis", "1000");
                        properties.setProperty("swarm.join.retryMaxDelayMillis", "60000");
                        StartupJoinPolicy.parse(properties);
                    }
                });
        suite.reproduction("startup-only policy preserves the legacy opt-out",
                new AllTests.CheckedRunnable() {
                    public void run() {
                        StartupJoinPolicy missing = StartupJoinPolicy.parse(
                                new java.util.Properties());
                        AllTests.assertTrue(!missing.isEnabled(),
                                "missing opt-in enabled startup retry");

                        java.util.Properties disabled = new java.util.Properties();
                        disabled.setProperty("swarm.join.startupRetry", " FaLsE ");
                        AllTests.assertTrue(!StartupJoinPolicy.parse(disabled).isEnabled(),
                                "explicit false enabled startup retry");
                    }
                });
        suite.reproduction("startup-only policy validates canonical booleans and bounds",
                new AllTests.CheckedRunnable() {
                    public void run() {
                        java.util.Properties standalone = validProperties();
                        standalone.setProperty("swarm.join.startupRetry", " TrUe ");
                        standalone.setProperty("swarm.join.minViewSize", "1");
                        StartupJoinPolicy policy = StartupJoinPolicy.parse(standalone);
                        AllTests.assertTrue(policy.isEnabled(),
                                "canonical true did not enable startup retry");
                        AllTests.assertEquals(Integer.valueOf(1),
                                Integer.valueOf(policy.getMinViewSize()),
                                "standalone minimum view was not retained");

                        expectInvalid(with(validProperties(),
                                "swarm.join.startupRetry", "yes"));
                        expectInvalid(with(validProperties(),
                                "swarm.join.minViewSize", "0"));
                        expectInvalid(with(validProperties(),
                                "swarm.join.minViewSize", "33"));
                        expectInvalid(with(validProperties(),
                                "swarm.join.minViewWaitMillis", "99"));
                        expectInvalid(with(validProperties(),
                                "swarm.join.minViewWaitMillis", "60001"));
                        expectInvalid(with(validProperties(),
                                "swarm.join.retryInitialDelayMillis", "999"));
                        expectInvalid(with(validProperties(),
                                "swarm.join.retryMaxDelayMillis", "999"));
                        expectInvalid(with(validProperties(),
                                "swarm.join.retryMaxDelayMillis", "59999"));
                        expectInvalid(with(validProperties(),
                                "swarm.join.retryMaxDelayMillis", "60001"));
                    }
                });
        suite.reproduction("equal jitter stays within the deterministic half-cap interval",
                new AllTests.CheckedRunnable() {
                    public void run() {
                        AllTests.assertEquals(Long.valueOf(30000L),
                                Long.valueOf(StartupChannelBootstrap.equalJitterDelay(
                                        60000L, 0.0d)),
                                "equal jitter lower bound changed");
                        long upper = StartupChannelBootstrap.equalJitterDelay(
                                60000L, 0.999999999d);
                        AllTests.assertTrue(upper >= 30000L && upper <= 60000L,
                                "equal jitter escaped its cap");
                    }
                });
        suite.reproduction("startup tunables and strict keys require a clean explicit opt-in",
                new AllTests.CheckedRunnable() {
                    public void run() {
                        java.util.Properties noOptIn = new java.util.Properties();
                        noOptIn.setProperty("swarm.join.minViewSize", "2");
                        expectInvalid(noOptIn);

                        java.util.Properties disabled = validProperties();
                        disabled.setProperty("swarm.join.startupRetry", "false");
                        expectInvalid(disabled);

                        String[] strictKeys = new String[] {
                            "swarm.join.strict",
                            "swarm.join.dbSource",
                            "swarm.join.validationQuery",
                            "swarm.join.validationQueryTimeoutSeconds",
                            "swarm.join.maxConnectTimeoutMillis",
                            "swarm.join.maxAttempts",
                            "swarm.session.stateProviderMode"
                        };
                        for (int i = 0; i < strictKeys.length; i++) {
                            java.util.Properties properties = validProperties();
                            properties.setProperty(strictKeys[i], "legacy-value");
                            expectInvalid(properties);
                        }
                    }
                });
        suite.reproduction("startup shutdown is independent from Application.stop",
                new AllTests.CheckedRunnable() {
                    public void run() {
                        final int[] installations = new int[1];
                        StartupShutdownHook hook = new StartupShutdownHook(
                                new StartupShutdownHook.HookInstaller() {
                                    public void addShutdownHook(Thread ignored) {
                                        installations[0]++;
                                    }
                                });
                        BootstrapState first = new BootstrapState();
                        BootstrapState second = new BootstrapState();
                        hook.register(first);
                        hook.register(second);
                        hook.signalShutdown();
                        AllTests.assertTrue(hook.isShuttingDown(),
                                "shutdown signal was not retained");
                        AllTests.assertEquals(Integer.valueOf(1),
                                Integer.valueOf(installations[0]),
                                "JVM shutdown hook was installed more than once");
                        AllTests.assertTrue(first.isCancelled() && second.isCancelled(),
                                "registered startup was not cancelled");
                    }
                });
        suite.reproduction("one waiter deregistration retains shared shutdown ownership",
                new AllTests.CheckedRunnable() {
                    public void run() {
                        StartupShutdownHook hook = new StartupShutdownHook(
                                new StartupShutdownHook.HookInstaller() {
                                    public void addShutdownHook(Thread ignored) {
                                    }
                                });
                        BootstrapState state = new BootstrapState();
                        hook.register(state);
                        hook.register(state);
                        hook.deregister(state);
                        hook.signalShutdown();
                        AllTests.assertTrue(state.isCancelled(),
                                "waiter deregistration orphaned bootstrap owner");
                    }
                });
        suite.reproduction("shutdown registration fails closed during JVM shutdown",
                new AllTests.CheckedRunnable() {
                    public void run() {
                        StartupShutdownHook hook = new StartupShutdownHook(
                                new StartupShutdownHook.HookInstaller() {
                                    public void addShutdownHook(Thread ignored) {
                                        throw new IllegalStateException("shutdown in progress");
                                    }
                                });
                        BootstrapState state = new BootstrapState();
                        hook.register(state);
                        AllTests.assertTrue(hook.isShuttingDown(),
                                "hook installation failure did not retain shutdown");
                        AllTests.assertTrue(state.isCancelled(),
                                "hook installation failure did not cancel startup");
                    }
                });
        suite.reproduction("shutdown hook installation failure uses bounded cleanup",
                new AllTests.CheckedRunnable() {
                    public void run() throws Exception {
                        final CountDownLatch entered = new CountDownLatch(1);
                        final CountDownLatch release = new CountDownLatch(1);
                        final AtomicInteger timeoutReports = new AtomicInteger();
                        StartupShutdownHook hook = new StartupShutdownHook(
                                new StartupShutdownHook.HookInstaller() {
                                    public void addShutdownHook(Thread ignored) {
                                        throw new IllegalStateException(
                                                "shutdown in progress");
                                    }
                                }, 50L, new StartupShutdownHook.TimeoutReporter() {
                                    public void deadlineExceeded() {
                                        timeoutReports.incrementAndGet();
                                    }
                                });
                        long started = System.currentTimeMillis();
                        hook.register(new StartupCancellation() {
                            public void cancel() {
                                entered.countDown();
                                boolean done = false;
                                while (!done) {
                                    try {
                                        release.await();
                                        done = true;
                                    } catch (InterruptedException ignored) {
                                    }
                                }
                            }
                        });
                        long elapsed = System.currentTimeMillis() - started;
                        AllTests.assertTrue(entered.getCount() == 0L,
                                "failed hook installation did not start cleanup");
                        AllTests.assertTrue(elapsed < 1000L,
                                "failed hook installation blocked beyond its deadline");
                        AllTests.assertEquals(Integer.valueOf(1),
                                Integer.valueOf(timeoutReports.get()),
                                "failed hook installation did not report its deadline");
                        release.countDown();
                    }
                });
        suite.reproduction("startup registered after shutdown is cancelled synchronously",
                new AllTests.CheckedRunnable() {
                    public void run() {
                        StartupShutdownHook hook = new StartupShutdownHook(
                                new StartupShutdownHook.HookInstaller() {
                                    public void addShutdownHook(Thread ignored) {
                                    }
                                });
                        hook.signalShutdown();
                        BootstrapState state = new BootstrapState();
                        hook.register(state);
                        AllTests.assertTrue(state.isCancelled(),
                                "late startup survived shutdown");
                    }
                });
        suite.reproduction("shutdown hook returns at its total cleanup deadline",
                new AllTests.CheckedRunnable() {
                    public void run() throws Exception {
                        final CountDownLatch entered = new CountDownLatch(1);
                        final CountDownLatch release = new CountDownLatch(1);
                        final AtomicInteger timeoutReports = new AtomicInteger();
                        StartupShutdownHook hook = new StartupShutdownHook(
                                new StartupShutdownHook.HookInstaller() {
                                    public void addShutdownHook(Thread ignored) {
                                    }
                                }, 50L, new StartupShutdownHook.TimeoutReporter() {
                                    public void deadlineExceeded() {
                                        timeoutReports.incrementAndGet();
                                    }
                                });
                        hook.register(new StartupCancellation() {
                            public void cancel() {
                                entered.countDown();
                                boolean done = false;
                                while (!done) {
                                    try {
                                        release.await();
                                        done = true;
                                    } catch (InterruptedException ignored) {
                                    }
                                }
                            }
                        });
                        long started = System.currentTimeMillis();
                        hook.signalShutdown();
                        long elapsed = System.currentTimeMillis() - started;
                        AllTests.assertTrue(entered.getCount() == 0L,
                                "shutdown cleanup task did not start");
                        AllTests.assertTrue(elapsed < 1000L,
                                "shutdown hook exceeded its cleanup deadline");
                        AllTests.assertEquals(Integer.valueOf(1),
                                Integer.valueOf(timeoutReports.get()),
                                "shutdown cleanup timeout was not reported exactly once");
                        release.countDown();
                    }
                });
        suite.reproduction("singleton timeout retries with a fresh channel",
                new AllTests.CheckedRunnable() {
                    public void run() throws Exception {
                        final List channels = new ArrayList();
                        final RecordingAdapterFactory adapters =
                                new RecordingAdapterFactory();
                        StartupChannelBootstrap.ChannelFactory channelFactory =
                                new StartupChannelBootstrap.ChannelFactory() {
                                    public Channel create(Application ignored) {
                                        if (channels.size() >= 2) {
                                            throw new AssertionError(
                                                    "bootstrap retried after the healthy candidate; starts="
                                                            + adapters.starts);
                                        }
                                        if (!channels.isEmpty()) {
                                            TestChannel previous = (TestChannel) channels.get(
                                                    channels.size() - 1);
                                            AllTests.assertTrue(!previous.isOpen(),
                                                    "next candidate was created before cleanup");
                                        }
                                        TestChannel channel = new TestChannel(
                                                channels.isEmpty() ? 1 : 2);
                                        channels.add(channel);
                                        return channel;
                                    }
                                };
                        TestScheduler scheduler = new TestScheduler();
                        StartupChannelBootstrap bootstrap = new StartupChannelBootstrap(
                                new StartupChannelBootstrap.Dependencies(
                                        channelFactory, adapters, scheduler,
                                        new StartupChannelBootstrap.Jitter() {
                                            public long delay(long capMillis) {
                                                return capMillis;
                                            }
                                        },
                                        new StartupChannelBootstrap.ClusterName() {
                                            public String get(Application ignored) {
                                                return "fresh-channel-test_swarm";
                                            }
                                        }));
                        java.util.Properties properties = validProperties();
                        properties.setProperty("swarm.join.minViewWaitMillis", "100");
                        BootstrapState state = new BootstrapState();
                        PullPushAdapter result = bootstrap.bootstrap(
                                new Application("fresh-channel-test"), state,
                                StartupJoinPolicy.parse(properties));

                        AllTests.assertEquals(Integer.valueOf(2),
                                Integer.valueOf(channels.size()),
                                "bootstrap did not create a fresh retry channel");
                        AllTests.assertEquals(Integer.valueOf(1),
                                Integer.valueOf(adapters.starts),
                                "adapter was started before the minimum view");
                        AllTests.assertTrue(result == adapters.last,
                                "bootstrap returned an unpublished adapter");
                        AllTests.assertTrue(!((TestChannel) channels.get(0)).isOpen(),
                                "timed-out channel remained open");
                        state.cancel();
                    }
                });
        suite.reproduction("connect exception retries with a fresh channel",
                new AllTests.CheckedRunnable() {
                    public void run() throws Exception {
                        final List channels = new ArrayList();
                        RecordingAdapterFactory adapters =
                                new RecordingAdapterFactory();
                        StartupChannelBootstrap bootstrap = testBootstrap(
                                new StartupChannelBootstrap.ChannelFactory() {
                                    public Channel create(Application ignored) {
                                        TestChannel channel = new TestChannel(2);
                                        channel.failConnect = channels.isEmpty();
                                        channels.add(channel);
                                        return channel;
                                    }
                                }, adapters);
                        BootstrapState state = new BootstrapState();
                        bootstrap.bootstrap(new Application("connect-retry-test"),
                                state, StartupJoinPolicy.parse(validProperties()));
                        AllTests.assertEquals(Integer.valueOf(2),
                                Integer.valueOf(channels.size()),
                                "connect failure reused its channel");
                        AllTests.assertTrue(!((TestChannel) channels.get(0)).isOpen(),
                                "connect-failed channel remained open");
                        state.cancel();
                    }
                });
        suite.reproduction("channel retry backoff caps at sixty seconds",
                new AllTests.CheckedRunnable() {
                    public void run() throws Exception {
                        final AtomicInteger creations = new AtomicInteger();
                        final List caps = new ArrayList();
                        StartupChannelBootstrap bootstrap =
                                new StartupChannelBootstrap(
                                        new StartupChannelBootstrap.Dependencies(
                                                new StartupChannelBootstrap.ChannelFactory() {
                                                    public Channel create(Application ignored) {
                                                        TestChannel channel =
                                                                new TestChannel(2);
                                                        channel.failConnect =
                                                                creations.incrementAndGet() < 8;
                                                        return channel;
                                                    }
                                                }, new RecordingAdapterFactory(),
                                                new TestScheduler(),
                                                new StartupChannelBootstrap.Jitter() {
                                                    public long delay(long capMillis) {
                                                        caps.add(Long.valueOf(capMillis));
                                                        return capMillis;
                                                    }
                                                }, new StartupChannelBootstrap.ClusterName() {
                                                    public String get(Application ignored) {
                                                        return "retry-cap-test_swarm";
                                                    }
                                                }));
                        BootstrapState state = new BootstrapState();
                        bootstrap.bootstrap(new Application("retry-cap-test"),
                                state, StartupJoinPolicy.parse(validProperties()));
                        long[] expected = new long[] {
                            1000L, 2000L, 4000L, 8000L,
                            16000L, 32000L, 60000L
                        };
                        AllTests.assertEquals(Integer.valueOf(expected.length),
                                Integer.valueOf(caps.size()),
                                "retry cap sequence length changed");
                        for (int i = 0; i < expected.length; i++) {
                            AllTests.assertEquals(Long.valueOf(expected[i]), caps.get(i),
                                    "retry cap changed at attempt " + i);
                        }
                        state.cancel();
                    }
                });
        suite.reproduction("cancel before candidate registration prevents channel creation",
                new AllTests.CheckedRunnable() {
                    public void run() throws Exception {
                        final AtomicInteger creations = new AtomicInteger();
                        StartupChannelBootstrap bootstrap = testBootstrap(
                                new StartupChannelBootstrap.ChannelFactory() {
                                    public Channel create(Application ignored) {
                                        creations.incrementAndGet();
                                        return new TestChannel(2);
                                    }
                                }, new RecordingAdapterFactory());
                        BootstrapState state = new BootstrapState();
                        state.cancel();
                        try {
                            bootstrap.bootstrap(new Application("pre-cancel-test"),
                                    state, StartupJoinPolicy.parse(validProperties()));
                            throw new AssertionError(
                                    "pre-cancelled bootstrap returned an adapter");
                        } catch (ChannelException expected) {
                            AllTests.assertEquals(Integer.valueOf(0),
                                    Integer.valueOf(creations.get()),
                                    "pre-cancelled bootstrap created a channel");
                        }
                    }
                });
        suite.reproduction("cancel during channel creation tracks failed local cleanup",
                new AllTests.CheckedRunnable() {
                    public void run() throws Exception {
                        final Application app = new Application(
                                "cancel-channel-creation-test");
                        final BootstrapState state =
                                ChannelUtils.getOrCreateBootstrapState(app);
                        final CountDownLatch creationEntered =
                                new CountDownLatch(1);
                        final CountDownLatch releaseCreation =
                                new CountDownLatch(1);
                        final TestChannel channel = new TestChannel(2);
                        channel.failClose = true;
                        final RecordingAdapterFactory adapters =
                                new RecordingAdapterFactory();
                        StartupChannelBootstrap bootstrap = testBootstrap(
                                new StartupChannelBootstrap.ChannelFactory() {
                                    public Channel create(Application ignored) {
                                        creationEntered.countDown();
                                        boolean released = false;
                                        while (!released) {
                                            try {
                                                releaseCreation.await();
                                                released = true;
                                            } catch (InterruptedException ignoredInterrupt) {
                                            }
                                        }
                                        return channel;
                                    }
                                }, adapters);
                        final AtomicReference failure = new AtomicReference();
                        Thread owner = bootstrapThread(bootstrap, state,
                                "cancel-channel-creation", failure);
                        owner.start();
                        AllTests.assertTrue(creationEntered.await(3000L,
                                        java.util.concurrent.TimeUnit.MILLISECONDS),
                                "channel creation did not start");
                        state.cancel();
                        releaseCreation.countDown();
                        owner.join(3000L);
                        AllTests.assertTrue(!owner.isAlive(),
                                "cancelled channel creator did not terminate");
                        AllTests.assertTrue(failure.get() instanceof ChannelException,
                                "cancelled channel creator did not fail");
                        AllTests.assertTrue(channel.closeAttempted,
                                "unregistered candidate cleanup was not attempted");
                        AllTests.assertTrue(
                                ChannelUtils.getOrCreateBootstrapState(app) == state,
                                "failed unregistered cleanup removed its tombstone");
                    }
                });
        suite.reproduction("adapter start failure is cleaned before a fresh retry",
                new AllTests.CheckedRunnable() {
                    public void run() throws Exception {
                        final AtomicInteger creations = new AtomicInteger();
                        final RecordingAdapterFactory adapters =
                                new RecordingAdapterFactory();
                        adapters.failStarts = 1;
                        StartupChannelBootstrap bootstrap = testBootstrap(
                                new StartupChannelBootstrap.ChannelFactory() {
                                    public Channel create(Application ignored) {
                                        creations.incrementAndGet();
                                        return new TestChannel(2);
                                    }
                                }, adapters);
                        BootstrapState state = new BootstrapState();
                        PullPushAdapter result = bootstrap.bootstrap(
                                new Application("adapter-start-test"), state,
                                StartupJoinPolicy.parse(validProperties()));

                        AllTests.assertEquals(Integer.valueOf(2),
                                Integer.valueOf(creations.get()),
                                "adapter start failure reused its channel");
                        AllTests.assertEquals(Integer.valueOf(2),
                                Integer.valueOf(adapters.starts),
                                "adapter start retry count changed");
                        AllTests.assertTrue(adapters.stops >= 1,
                                "failed adapter was not stopped");
                        AllTests.assertTrue(result == adapters.last,
                                "retry did not publish the healthy adapter");
                        state.cancel();
                    }
                });
        suite.reproduction("concurrent startup callers share one published adapter",
                new AllTests.CheckedRunnable() {
                    public void run() throws Exception {
                        final AtomicInteger creations = new AtomicInteger();
                        final RecordingAdapterFactory adapters =
                                new RecordingAdapterFactory();
                        final StartupChannelBootstrap bootstrap = testBootstrap(
                                new StartupChannelBootstrap.ChannelFactory() {
                                    public Channel create(Application ignored) {
                                        creations.incrementAndGet();
                                        return new TestChannel(2);
                                    }
                                }, adapters);
                        final BootstrapState state = new BootstrapState();
                        final StartupJoinPolicy policy = StartupJoinPolicy.parse(
                                validProperties());
                        final PullPushAdapter[] results = new PullPushAdapter[32];
                        final AtomicReference failure = new AtomicReference();
                        final CountDownLatch ready = new CountDownLatch(results.length);
                        final CountDownLatch go = new CountDownLatch(1);
                        Thread[] callers = new Thread[results.length];
                        for (int i = 0; i < callers.length; i++) {
                            final int index = i;
                            callers[i] = new Thread(new Runnable() {
                                public void run() {
                                    ready.countDown();
                                    try {
                                        go.await();
                                        results[index] = bootstrap.bootstrap(
                                                new Application("concurrent-test"),
                                                state, policy);
                                    } catch (Throwable problem) {
                                        failure.compareAndSet(null, problem);
                                    }
                                }
                            }, "bootstrap-caller-" + i);
                            callers[i].start();
                        }
                        ready.await();
                        go.countDown();
                        for (int i = 0; i < callers.length; i++) {
                            callers[i].join(3000L);
                            AllTests.assertTrue(!callers[i].isAlive(),
                                    "concurrent bootstrap caller did not finish");
                        }
                        AllTests.assertTrue(failure.get() == null,
                                "concurrent bootstrap failed: " + failure.get());
                        for (int i = 1; i < results.length; i++) {
                            AllTests.assertTrue(results[i] == results[0],
                                    "callers received different adapters");
                        }
                        AllTests.assertEquals(Integer.valueOf(1),
                                Integer.valueOf(creations.get()),
                                "concurrent callers created multiple channels");
                        state.cancel();
                    }
                });
        suite.reproduction("interrupted bootstrap waiter fail-stops until cancellation",
                new AllTests.CheckedRunnable() {
                    public void run() throws Exception {
                        final BlockingAdapterFactory adapters =
                                new BlockingAdapterFactory();
                        final StartupChannelBootstrap bootstrap = testBootstrap(
                                new StartupChannelBootstrap.ChannelFactory() {
                                    public Channel create(Application ignored) {
                                        return new TestChannel(2);
                                    }
                                }, adapters);
                        final BootstrapState state = new BootstrapState();
                        final AtomicReference ownerFailure = new AtomicReference();
                        final AtomicReference waiterFailure = new AtomicReference();
                        final AtomicReference waiterInterrupted = new AtomicReference();
                        Thread owner = bootstrapThread(bootstrap, state,
                                "interrupted-waiter-owner", ownerFailure);
                        owner.start();
                        AllTests.assertTrue(adapters.startEntered.await(3000L,
                                        java.util.concurrent.TimeUnit.MILLISECONDS),
                                "bootstrap owner did not reach adapter start");
                        Thread waiter = new Thread(new Runnable() {
                            public void run() {
                                try {
                                    bootstrap.bootstrap(
                                            new Application("interrupted-waiter"), state,
                                            StartupJoinPolicy.parse(validProperties()));
                                } catch (Throwable problem) {
                                    waiterFailure.set(problem);
                                    waiterInterrupted.set(Boolean.valueOf(
                                            Thread.currentThread().isInterrupted()));
                                }
                            }
                        }, "interrupted-bootstrap-waiter");
                        waiter.start();
                        Thread.sleep(100L);
                        waiter.interrupt();
                        Thread.sleep(100L);
                        AllTests.assertTrue(waiter.isAlive(),
                                "interrupted waiter bypassed fail-stop");
                        state.cancel();
                        adapters.release.countDown();
                        waiter.join(3000L);
                        owner.join(3000L);
                        AllTests.assertTrue(!waiter.isAlive() && !owner.isAlive(),
                                "cancelled waiter or owner did not finish");
                        AllTests.assertTrue(waiterFailure.get() instanceof ChannelException,
                                "waiter did not return a channel failure");
                        AllTests.assertEquals(Boolean.TRUE, waiterInterrupted.get(),
                                "waiter interrupt flag was not restored");
                    }
                });
        suite.reproduction("invalid config waiter restores interrupt after cancellation",
                new AllTests.CheckedRunnable() {
                    public void run() throws Exception {
                        java.util.Properties invalid = validProperties();
                        invalid.setProperty("swarm.join.startupRetry", "false");
                        final Application app = applicationWithProperties(
                                "invalid-config-interrupt-test", invalid);
                        final BootstrapState state =
                                ChannelUtils.getOrCreateBootstrapState(app);
                        Thread leader = new Thread(new Runnable() {
                            public void run() {
                                try {
                                    ChannelUtils.getAdapter(app);
                                } catch (ChannelException expected) {
                                }
                            }
                        }, "invalid-config-leader");
                        leader.start();
                        awaitLeader(state);

                        final AtomicReference waiterFailure =
                                new AtomicReference();
                        final AtomicBoolean waiterInterrupted =
                                new AtomicBoolean();
                        Thread waiter = new Thread(new Runnable() {
                            public void run() {
                                try {
                                    ChannelUtils.getAdapter(app);
                                    waiterFailure.set(new AssertionError(
                                            "invalid configuration returned an adapter"));
                                } catch (ChannelException expected) {
                                    waiterInterrupted.set(
                                            Thread.currentThread().isInterrupted());
                                } catch (Throwable problem) {
                                    waiterFailure.set(problem);
                                }
                            }
                        }, "invalid-config-waiter");
                        waiter.start();
                        Thread.sleep(100L);
                        waiter.interrupt();
                        Thread.sleep(100L);
                        AllTests.assertTrue(waiter.isAlive(),
                                "invalid-config waiter escaped fail-stop");
                        state.cancel();
                        waiter.join(3000L);
                        leader.join(3000L);
                        AllTests.assertTrue(!waiter.isAlive() && !leader.isAlive(),
                                "cancelled invalid-config threads did not finish");
                        AllTests.assertTrue(waiterFailure.get() == null,
                                "invalid-config waiter failed: "
                                        + waiterFailure.get());
                        AllTests.assertTrue(waiterInterrupted.get(),
                                "invalid-config waiter lost its interrupt flag");
                    }
                });
        suite.reproduction("different applications bootstrap in parallel",
                new AllTests.CheckedRunnable() {
                    public void run() throws Exception {
                        final CountDownLatch bothCreated = new CountDownLatch(2);
                        final CountDownLatch release = new CountDownLatch(1);
                        final AtomicReference failure = new AtomicReference();
                        final BootstrapState firstState = new BootstrapState();
                        final BootstrapState secondState = new BootstrapState();
                        final RecordingAdapterFactory adapters =
                                new RecordingAdapterFactory();
                        final StartupChannelBootstrap bootstrap = testBootstrap(
                                new StartupChannelBootstrap.ChannelFactory() {
                                    public Channel create(Application ignored)
                                            throws Exception {
                                        bothCreated.countDown();
                                        release.await();
                                        return new TestChannel(2);
                                    }
                                }, adapters);
                        Thread first = bootstrapThread(bootstrap, firstState,
                                "parallel-app-a", failure);
                        Thread second = bootstrapThread(bootstrap, secondState,
                                "parallel-app-b", failure);
                        first.start();
                        second.start();
                        AllTests.assertTrue(bothCreated.await(3000L,
                                        java.util.concurrent.TimeUnit.MILLISECONDS),
                                "application bootstraps serialized channel creation");
                        release.countDown();
                        first.join(3000L);
                        second.join(3000L);
                        AllTests.assertTrue(!first.isAlive() && !second.isAlive(),
                                "parallel application bootstrap did not finish");
                        AllTests.assertTrue(failure.get() == null,
                                "parallel application bootstrap failed: "
                                        + failure.get());
                        firstState.cancel();
                        secondState.cancel();
                    }
                });
        suite.reproduction("shutdown cancellation wins against publication",
                new AllTests.CheckedRunnable() {
                    public void run() throws Exception {
                        final BlockingAdapterFactory adapters =
                                new BlockingAdapterFactory();
                        final TestChannel channel = new TestChannel(2);
                        final StartupChannelBootstrap bootstrap = testBootstrap(
                                new StartupChannelBootstrap.ChannelFactory() {
                                    public Channel create(Application ignored) {
                                        return channel;
                                    }
                                }, adapters);
                        final BootstrapState state = new BootstrapState();
                        final AtomicReference result = new AtomicReference();
                        final AtomicReference failure = new AtomicReference();
                        Thread owner = new Thread(new Runnable() {
                            public void run() {
                                try {
                                    result.set(bootstrap.bootstrap(
                                            new Application("cancel-publish-test"),
                                            state, StartupJoinPolicy.parse(
                                                    validProperties())));
                                } catch (Throwable problem) {
                                    failure.set(problem);
                                }
                            }
                        }, "bootstrap-cancel-owner");
                        owner.start();
                        adapters.startEntered.await();
                        state.cancel();
                        adapters.release.countDown();
                        owner.join(3000L);

                        AllTests.assertTrue(!owner.isAlive(),
                                "cancelled bootstrap owner did not finish");
                        AllTests.assertTrue(result.get() == null,
                                "cancelled candidate was published");
                        AllTests.assertTrue(failure.get() instanceof ChannelException,
                                "cancelled bootstrap did not fail as a channel operation");
                        AllTests.assertTrue(!channel.isOpen(),
                                "cancelled candidate remained open");
                    }
                });
        suite.reproduction("cancel before registry publication cannot expose closed adapter",
                new AllTests.CheckedRunnable() {
                    public void run() throws Exception {
                        Application app = new Application(
                                "cancel-before-registry-publication");
                        BootstrapState state = ChannelUtils.createBootstrapState(app);
                        TestChannel channel = new TestChannel(2);
                        RecordingAdapterFactory adapters =
                                new RecordingAdapterFactory();
                        StartupCandidate candidate = new StartupCandidate(
                                channel, adapters);
                        AllTests.assertTrue(state.claimLeader(),
                                "test could not claim bootstrap ownership");
                        AllTests.assertTrue(state.registerCandidate(candidate),
                                "test could not register candidate");
                        channel.connect("test_swarm");
                        PullPushAdapter adapter = adapters.create(channel);
                        candidate.attachAdapter(adapter);
                        candidate.startAdapter();
                        AllTests.assertTrue(state.publish(candidate),
                                "test could not publish candidate state");

                        state.cancel();
                        AllTests.assertTrue(!ChannelUtils.publishAdapter(
                                        app, state, adapter),
                                "cancelled state was inserted into adapter registry");
                        AllTests.assertTrue(!ChannelUtils.adapters.containsKey(app),
                                "closed adapter remained visible in registry");
                        state.leaderFinished();
                    }
                });
        suite.reproduction("cancelled state remains a tombstone until cleanup and leader finish",
                new AllTests.CheckedRunnable() {
                    public void run() throws Exception {
                        final Application app = new Application(
                                "cancel-cleanup-tombstone");
                        final BootstrapState state = ChannelUtils.getOrCreateBootstrapState(app);
                        final CountDownLatch published = new CountDownLatch(1);
                        final CountDownLatch finishLeader = new CountDownLatch(1);
                        final CountDownLatch cleanupEntered = new CountDownLatch(1);
                        final CountDownLatch finishCleanup = new CountDownLatch(1);
                        final AtomicReference failure = new AtomicReference();
                        final TestChannel channel = new TestChannel(2);
                        final RecordingAdapterFactory adapters =
                                new RecordingAdapterFactory() {
                                    public void stop(PullPushAdapter ignored) {
                                        cleanupEntered.countDown();
                                        try {
                                            finishCleanup.await();
                                        } catch (InterruptedException interrupted) {
                                            Thread.currentThread().interrupt();
                                        }
                                        super.stop(ignored);
                                    }
                                };

                        Thread owner = new Thread(new Runnable() {
                            public void run() {
                                try {
                                    AllTests.assertTrue(state.claimLeader(),
                                            "test could not claim bootstrap ownership");
                                    StartupCandidate candidate = new StartupCandidate(
                                            channel, adapters);
                                    AllTests.assertTrue(state.registerCandidate(candidate),
                                            "test could not register candidate");
                                    channel.connect("test_swarm");
                                    PullPushAdapter adapter = adapters.create(channel);
                                    candidate.attachAdapter(adapter);
                                    candidate.startAdapter();
                                    AllTests.assertTrue(state.publish(candidate),
                                            "test could not publish candidate state");
                                    published.countDown();
                                    boolean interrupted = false;
                                    while (true) {
                                        try {
                                            finishLeader.await();
                                            break;
                                        } catch (InterruptedException expected) {
                                            interrupted = true;
                                        }
                                    }
                                    if (interrupted) {
                                        Thread.currentThread().interrupt();
                                    }
                                } catch (Throwable problem) {
                                    failure.compareAndSet(null, problem);
                                } finally {
                                    state.leaderFinished();
                                }
                            }
                        }, "tombstone-owner");
                        owner.start();
                        AllTests.assertTrue(published.await(3000L,
                                        java.util.concurrent.TimeUnit.MILLISECONDS),
                                "test candidate was not published");

                        Thread canceller = new Thread(new Runnable() {
                            public void run() {
                                state.cancel();
                            }
                        }, "tombstone-canceller");
                        canceller.start();
                        AllTests.assertTrue(cleanupEntered.await(3000L,
                                        java.util.concurrent.TimeUnit.MILLISECONDS),
                                "cancel cleanup did not start");
                        AllTests.assertTrue(ChannelUtils.getOrCreateBootstrapState(app) == state,
                                "late caller replaced state during cleanup");

                        finishCleanup.countDown();
                        canceller.join(3000L);
                        AllTests.assertTrue(!canceller.isAlive(),
                                "cancel cleanup did not finish");
                        AllTests.assertTrue(ChannelUtils.getOrCreateBootstrapState(app) == state,
                                "late caller replaced state before leader finished");

                        finishLeader.countDown();
                        owner.join(3000L);
                        AllTests.assertTrue(!owner.isAlive(),
                                "bootstrap leader did not finish");
                        AllTests.assertTrue(failure.get() == null,
                                "tombstone setup failed: " + failure.get());
                        AllTests.assertTrue(ChannelUtils.getOrCreateBootstrapState(app) != state,
                                "completed tombstone was not removed");
                        ChannelUtils.stopAdapter(app);
                    }
                });
        suite.reproduction("candidate cleanup continues after individual failures",
                new AllTests.CheckedRunnable() {
                    public void run() throws Exception {
                        TestChannel channel = new TestChannel(2);
                        channel.connect("cleanup-test");
                        channel.failDisconnect = true;
                        RecordingAdapterFactory adapters =
                                new RecordingAdapterFactory();
                        adapters.failStop = true;
                        StartupCandidate candidate = new StartupCandidate(channel, adapters);
                        candidate.attachAdapter(new PullPushAdapter(
                                channel, null, null, false));
                        boolean cleaned = candidate.cleanup();

                        AllTests.assertTrue(!cleaned,
                                "failed candidate cleanup reported success");
                        AllTests.assertTrue(channel.disconnectAttempted,
                                "candidate disconnect was not attempted");
                        AllTests.assertTrue(channel.closeAttempted,
                                "candidate close was skipped after cleanup failure");
                    }
                });
        suite.reproduction("incomplete candidate cleanup fail-stops before retry",
                new AllTests.CheckedRunnable() {
                    public void run() throws Exception {
                        final AtomicInteger creations = new AtomicInteger();
                        final CountDownLatch cleanupAttempted = new CountDownLatch(1);
                        final RecordingAdapterFactory adapters =
                                new RecordingAdapterFactory() {
                                    public void stop(PullPushAdapter ignored) {
                                        cleanupAttempted.countDown();
                                        throw new IllegalStateException(
                                                "simulated adapter stop failure");
                                    }
                                };
                        adapters.failStarts = 1;
                        final StartupChannelBootstrap bootstrap = testBootstrap(
                                new StartupChannelBootstrap.ChannelFactory() {
                                    public Channel create(Application ignored) {
                                        creations.incrementAndGet();
                                        return new TestChannel(2);
                                    }
                                }, adapters);
                        final Application app = new Application(
                                "cleanup-fail-stop-registry");
                        final BootstrapState state =
                                ChannelUtils.getOrCreateBootstrapState(app);
                        final AtomicReference failure = new AtomicReference();
                        Thread owner = bootstrapThread(bootstrap, state,
                                "cleanup-fail-stop", failure);
                        owner.start();
                        AllTests.assertTrue(cleanupAttempted.await(3000L,
                                        java.util.concurrent.TimeUnit.MILLISECONDS),
                                "candidate cleanup was not attempted");
                        Thread.sleep(100L);
                        AllTests.assertEquals(Integer.valueOf(1),
                                Integer.valueOf(creations.get()),
                                "bootstrap retried after incomplete cleanup");
                        AllTests.assertTrue(owner.isAlive(),
                                "incomplete cleanup did not enter fail-stop");
                        state.cancel();
                        owner.join(3000L);
                        AllTests.assertTrue(!owner.isAlive(),
                                "cancelled cleanup fail-stop did not finish");
                        AllTests.assertTrue(failure.get() instanceof ChannelException,
                                "cleanup fail-stop did not return channel failure");
                        AllTests.assertTrue(
                                ChannelUtils.getOrCreateBootstrapState(app) == state,
                                "failed detached cleanup removed its tombstone");
                    }
                });
        suite.reproduction("cancel cannot complete while detached cleanup is active",
                new AllTests.CheckedRunnable() {
                    public void run() throws Exception {
                        final Application app = new Application(
                                "detached-cleanup-cancel-race-test");
                        final BootstrapState state =
                                ChannelUtils.getOrCreateBootstrapState(app);
                        final BlockingFailStopAdapterFactory adapters =
                                new BlockingFailStopAdapterFactory();
                        Thread owner = new Thread(new Runnable() {
                            public void run() {
                                try {
                                    state.claimLeader();
                                    TestChannel channel = new TestChannel(2);
                                    StartupCandidate candidate =
                                            new StartupCandidate(channel, adapters);
                                    state.registerCandidate(candidate);
                                    channel.connect("test_swarm");
                                    PullPushAdapter adapter =
                                            adapters.create(channel);
                                    candidate.attachAdapter(adapter);
                                    StartupCandidate owned =
                                            state.takeCandidate(candidate);
                                    state.recordCleanupResult(owned.cleanup());
                                } catch (Exception problem) {
                                    throw new RuntimeException(problem);
                                } finally {
                                    state.leaderFinished();
                                }
                            }
                        }, "detached-cleanup-owner");
                        owner.start();
                        AllTests.assertTrue(adapters.stopEntered.await(3000L,
                                        java.util.concurrent.TimeUnit.MILLISECONDS),
                                "detached cleanup did not start");
                        state.cancel();
                        AllTests.assertTrue(
                                ChannelUtils.getOrCreateBootstrapState(app) == state,
                                "cancel removed a running detached cleanup");
                        adapters.release.countDown();
                        owner.join(3000L);
                        AllTests.assertTrue(!owner.isAlive(),
                                "detached cleanup owner did not terminate");
                        AllTests.assertTrue(
                                ChannelUtils.getOrCreateBootstrapState(app) == state,
                                "failed detached cleanup lost its tombstone after cancel");
                    }
                });
        suite.reproduction("published adapter is retained after runtime view loss",
                new AllTests.CheckedRunnable() {
                    public void run() throws Exception {
                        final AtomicInteger creations = new AtomicInteger();
                        final TestChannel channel = new TestChannel(2);
                        RecordingAdapterFactory adapters =
                                new RecordingAdapterFactory();
                        StartupChannelBootstrap bootstrap = testBootstrap(
                                new StartupChannelBootstrap.ChannelFactory() {
                                    public Channel create(Application ignored) {
                                        creations.incrementAndGet();
                                        return channel;
                                    }
                                }, adapters);
                        BootstrapState state = new BootstrapState();
                        StartupJoinPolicy policy = StartupJoinPolicy.parse(
                                validProperties());
                        PullPushAdapter first = bootstrap.bootstrap(
                                new Application("runtime-view-test"), state, policy);
                        channel.setMembers(1);
                        PullPushAdapter second = bootstrap.bootstrap(
                                new Application("runtime-view-test"), state, policy);

                        AllTests.assertTrue(first == second,
                                "runtime view loss replaced the adapter");
                        AllTests.assertEquals(Integer.valueOf(1),
                                Integer.valueOf(creations.get()),
                                "runtime view loss triggered a fresh channel");
                        state.cancel();
                    }
                });
        suite.reproduction("production registry reuses one adapter for all consumers",
                new AllTests.CheckedRunnable() {
                    public void run() throws Exception {
                        Application app = new Application("consumer-reuse-test");
                        BootstrapState state = ChannelUtils.getOrCreateBootstrapState(app);
                        TestChannel channel = new TestChannel(2);
                        RecordingAdapterFactory adapters =
                                new RecordingAdapterFactory();
                        AllTests.assertTrue(state.claimLeader(),
                                "test could not claim registry bootstrap");
                        StartupCandidate candidate = new StartupCandidate(
                                channel, adapters);
                        state.registerCandidate(candidate);
                        channel.connect("test_swarm");
                        PullPushAdapter adapter = adapters.create(channel);
                        candidate.attachAdapter(adapter);
                        candidate.startAdapter();
                        state.publish(candidate);
                        AllTests.assertTrue(ChannelUtils.publishAdapter(
                                        app, state, adapter),
                                "test could not publish production adapter");
                        state.leaderFinished();

                        PullPushAdapter cache = ChannelUtils.getAdapter(app);
                        PullPushAdapter idGenerator = ChannelUtils.getAdapter(app);
                        PullPushAdapter sessions = ChannelUtils.getAdapter(app);
                        AllTests.assertTrue(cache == adapter
                                        && idGenerator == adapter
                                        && sessions == adapter,
                                "production consumers received different adapters");
                        ChannelUtils.stopAdapter(app);
                    }
                });
        suite.reproduction("stop serializes published state before registry removal",
                new AllTests.CheckedRunnable() {
                    public void run() throws Exception {
                        final Application app = new Application(
                                "stop-publication-race-test");
                        final BootstrapState state =
                                ChannelUtils.getOrCreateBootstrapState(app);
                        TestChannel channel = new TestChannel(2);
                        RecordingAdapterFactory adapters =
                                new RecordingAdapterFactory();
                        AllTests.assertTrue(state.claimLeader(),
                                "test could not claim registry bootstrap");
                        StartupCandidate candidate = new StartupCandidate(
                                channel, adapters);
                        state.registerCandidate(candidate);
                        channel.connect("test_swarm");
                        final PullPushAdapter adapter = adapters.create(channel);
                        candidate.attachAdapter(adapter);
                        candidate.startAdapter();
                        state.publish(candidate);
                        AllTests.assertTrue(ChannelUtils.publishAdapter(
                                        app, state, adapter),
                                "test could not publish production adapter");
                        state.leaderFinished();

                        final CountDownLatch publicationEntered =
                                new CountDownLatch(1);
                        final CountDownLatch releasePublication =
                                new CountDownLatch(1);
                        Thread publication = new Thread(new Runnable() {
                            public void run() {
                                state.publishToRegistry(
                                        new BootstrapState.PublicationAction() {
                                            public boolean publish(
                                                    PullPushAdapter ignored) {
                                                publicationEntered.countDown();
                                                try {
                                                    releasePublication.await();
                                                } catch (InterruptedException problem) {
                                                    Thread.currentThread().interrupt();
                                                }
                                                return true;
                                            }
                                        });
                            }
                        }, "held-publication");
                        publication.start();
                        AllTests.assertTrue(publicationEntered.await(3000L,
                                        java.util.concurrent.TimeUnit.MILLISECONDS),
                                "publication gate was not held");

                        Thread stop = new Thread(new Runnable() {
                            public void run() {
                                ChannelUtils.stopAdapter(app);
                            }
                        }, "concurrent-stop");
                        stop.start();
                        awaitBlocked(stop);
                        AllTests.assertTrue(ChannelUtils.adapters.get(app) == adapter,
                                "stop removed the adapter before serializing cancellation");

                        releasePublication.countDown();
                        publication.join(3000L);
                        stop.join(3000L);
                        AllTests.assertTrue(!publication.isAlive() && !stop.isAlive(),
                                "publication/stop race did not terminate");
                        AllTests.assertTrue(ChannelUtils.adapters.get(app) == null,
                                "cancelled adapter remained in the registry");
                        AllTests.assertTrue(!channel.isOpen(),
                                "cancelled published channel remained open");
                    }
                });
        suite.reproduction("failed published cleanup retains its tombstone",
                new AllTests.CheckedRunnable() {
                    public void run() throws Exception {
                        Application app = applicationWithProperties(
                                "failed-published-cleanup-test", validProperties());
                        BootstrapState state =
                                ChannelUtils.getOrCreateBootstrapState(app);
                        TestChannel channel = new TestChannel(2);
                        RecordingAdapterFactory adapters =
                                new RecordingAdapterFactory();
                        adapters.failStop = true;
                        AllTests.assertTrue(state.claimLeader(),
                                "test could not claim registry bootstrap");
                        StartupCandidate candidate = new StartupCandidate(
                                channel, adapters);
                        state.registerCandidate(candidate);
                        channel.connect("test_swarm");
                        PullPushAdapter adapter = adapters.create(channel);
                        candidate.attachAdapter(adapter);
                        candidate.startAdapter();
                        state.publish(candidate);
                        AllTests.assertTrue(ChannelUtils.publishAdapter(
                                        app, state, adapter),
                                "test could not publish production adapter");
                        state.leaderFinished();

                        ChannelUtils.stopAdapter(app);
                        AllTests.assertTrue(
                                ChannelUtils.getOrCreateBootstrapState(app) == state,
                                "failed published cleanup removed its tombstone");
                        try {
                            ChannelUtils.getAdapter(app);
                            throw new AssertionError(
                                    "late caller passed a failed cleanup tombstone");
                        } catch (ChannelException expected) {
                        }
                        AllTests.assertTrue(
                                ChannelUtils.getOrCreateBootstrapState(app) == state,
                                "late caller replaced a failed cleanup tombstone");
                    }
                });
    }

    private static void awaitBlocked(Thread thread) throws Exception {
        long deadline = System.currentTimeMillis() + 3000L;
        while (thread.getState() != Thread.State.BLOCKED
                && System.currentTimeMillis() < deadline) {
            Thread.sleep(5L);
        }
        AllTests.assertEquals(Thread.State.BLOCKED, thread.getState(),
                "stop did not block on the held publication gate");
    }

    private static void awaitLeader(BootstrapState state) throws Exception {
        long deadline = System.currentTimeMillis() + 3000L;
        while (!state.hasLeader() && System.currentTimeMillis() < deadline) {
            Thread.sleep(5L);
        }
        AllTests.assertTrue(state.hasLeader(),
                "bootstrap leader was not established");
    }

    private static Application applicationWithProperties(
            String name, java.util.Properties source) throws Exception {
        Application app = new Application(name);
        ResourceProperties properties = new ResourceProperties();
        properties.putAll(source);
        Field field = Application.class.getDeclaredField("props");
        field.setAccessible(true);
        field.set(app, properties);
        Field eventLog = Application.class.getDeclaredField("eventLog");
        eventLog.setAccessible(true);
        eventLog.set(app, LogFactory.getLog(
                StartupChannelBootstrapTest.class));
        return app;
    }

    private static java.util.Properties validProperties() {
        java.util.Properties properties = new java.util.Properties();
        properties.setProperty("swarm.join.startupRetry", "true");
        properties.setProperty("swarm.join.minViewSize", "2");
        properties.setProperty("swarm.join.minViewWaitMillis", "10000");
        properties.setProperty("swarm.join.retryInitialDelayMillis", "1000");
        properties.setProperty("swarm.join.retryMaxDelayMillis", "60000");
        return properties;
    }

    private static java.util.Properties with(java.util.Properties source,
                                              String name, String value) {
        source.setProperty(name, value);
        return source;
    }

    private static void expectInvalid(java.util.Properties properties) {
        try {
            StartupJoinPolicy.parse(properties);
        } catch (IllegalArgumentException expected) {
            return;
        }
        throw new AssertionError("invalid startup-only profile was accepted");
    }

    private static StartupChannelBootstrap testBootstrap(
            StartupChannelBootstrap.ChannelFactory channels,
            RecordingAdapterFactory adapters) {
        return new StartupChannelBootstrap(
                new StartupChannelBootstrap.Dependencies(
                        channels, adapters, new TestScheduler(),
                        new StartupChannelBootstrap.Jitter() {
                            public long delay(long capMillis) {
                                return capMillis;
                            }
                        },
                        new StartupChannelBootstrap.ClusterName() {
                            public String get(Application ignored) {
                                return "test_swarm";
                            }
                        }));
    }

    private static Thread bootstrapThread(
            final StartupChannelBootstrap bootstrap,
            final BootstrapState state, final String appName,
            final AtomicReference failure) {
        return new Thread(new Runnable() {
            public void run() {
                try {
                    bootstrap.bootstrap(new Application(appName), state,
                            StartupJoinPolicy.parse(validProperties()));
                } catch (Throwable problem) {
                    failure.compareAndSet(null, problem);
                }
            }
        }, appName + "-bootstrap");
    }

    private static class RecordingAdapterFactory
            implements StartupChannelBootstrap.AdapterFactory {
        int starts;
        int stops;
        int failStarts;
        boolean failStop;
        PullPushAdapter last;

        public PullPushAdapter create(Channel channel) {
            last = new PullPushAdapter(channel, null, null, false);
            return last;
        }

        public void start(PullPushAdapter adapter) throws Exception {
            starts++;
            if (failStarts > 0) {
                failStarts--;
                throw new Exception("simulated adapter start failure");
            }
        }

        public void stop(PullPushAdapter ignored) {
            stops++;
            if (failStop) {
                throw new IllegalStateException("simulated adapter stop failure");
            }
        }
    }

    private static final class BlockingAdapterFactory
            extends RecordingAdapterFactory {
        final CountDownLatch startEntered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);

        public void start(PullPushAdapter adapter) throws Exception {
            starts++;
            startEntered.countDown();
            release.await();
        }
    }

    private static final class BlockingFailStopAdapterFactory
            extends RecordingAdapterFactory {
        final CountDownLatch stopEntered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);

        public void stop(PullPushAdapter ignored) {
            stops++;
            stopEntered.countDown();
            boolean done = false;
            while (!done) {
                try {
                    release.await();
                    done = true;
                } catch (InterruptedException ignoredInterrupt) {
                }
            }
            throw new IllegalStateException("simulated cleanup failure");
        }
    }

    private static final class TestScheduler
            implements StartupChannelBootstrap.Scheduler {
        long now;

        public long nowMillis() {
            return now;
        }

        public void sleep(long millis) {
            now += millis;
        }
    }

    private static final class TestChannel extends JChannel {
        private View view;
        boolean disconnectAttempted;
        boolean closeAttempted;
        boolean failConnect;
        boolean failDisconnect;
        boolean failClose;

        TestChannel(int members) {
            super(false);
            setMembers(members);
        }

        void setMembers(int members) {
            Vector addresses = new Vector();
            for (int i = 0; i < members; i++) {
                addresses.add(new IpAddress(7800 + i));
            }
            view = new View((org.jgroups.Address) addresses.firstElement(),
                    1L, addresses);
        }

        public synchronized void connect(String ignored) throws ChannelException {
            connected = true;
            closed = false;
            if (failConnect) {
                throw new ChannelException("simulated connect failure");
            }
        }

        public View getView() {
            return view;
        }

        public synchronized void disconnect() {
            disconnectAttempted = true;
            connected = false;
            if (failDisconnect) {
                throw new IllegalStateException(
                        "simulated channel disconnect failure");
            }
        }

        public synchronized void close() {
            closeAttempted = true;
            if (failClose) {
                throw new IllegalStateException(
                        "simulated channel close failure");
            }
            connected = false;
            closed = true;
        }
    }
}
