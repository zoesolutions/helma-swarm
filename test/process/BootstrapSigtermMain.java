package helma.swarm;

import helma.framework.core.Application;
import org.apache.commons.logging.LogFactory;
import org.jgroups.Address;
import org.jgroups.Channel;
import org.jgroups.ChannelException;
import org.jgroups.JChannel;
import org.jgroups.View;
import org.jgroups.blocks.PullPushAdapter;
import org.jgroups.stack.IpAddress;
import helma.util.ResourceProperties;

import java.util.Properties;
import java.util.Vector;
import java.lang.reflect.Field;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public final class BootstrapSigtermMain {

    private BootstrapSigtermMain() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.exit(2);
        }
        String mode = args[0];
        if ("invalid-config".equals(mode)) {
            invalidConfiguration();
        } else if ("invalid-config-waiters".equals(mode)) {
            invalidConfigurationWaiters();
        } else if ("session-wait".equals(mode)) {
            sessionWait();
        } else {
            channelBootstrap(mode);
        }
    }

    private static StartupShutdownHook runtimeHook() {
        return new StartupShutdownHook(
                new StartupShutdownHook.HookInstaller() {
                    public void addShutdownHook(Thread thread) {
                        Runtime.getRuntime().addShutdownHook(thread);
                    }
                }, 500L);
    }

    private static void invalidConfiguration() throws Exception {
        final Application app = invalidApplication("sigterm-invalid-config");
        final BootstrapState state = ChannelUtils.getOrCreateBootstrapState(app);
        Thread caller = new Thread(new Runnable() {
            public void run() {
                try {
                    ChannelUtils.getAdapter(app);
                } catch (ChannelException expected) {
                }
            }
        }, "invalid-config-caller");
        caller.start();
        awaitLeader(state);
        ready("invalid-config");
        caller.join();
    }

    private static void invalidConfigurationWaiters() throws Exception {
        final Application app = invalidApplication(
                "sigterm-invalid-config-waiters");
        final BootstrapState state = ChannelUtils.getOrCreateBootstrapState(app);
        final CountDownLatch started = new CountDownLatch(32);
        for (int i = 0; i < 32; i++) {
            Thread caller = new Thread(new Runnable() {
                public void run() {
                    started.countDown();
                    try {
                        ChannelUtils.getAdapter(app);
                    } catch (ChannelException expected) {
                    }
                }
            }, "invalid-config-waiter-" + i);
            caller.start();
        }
        started.await();
        awaitLeader(state);
        Thread.sleep(100L);
        ready("invalid-config-waiters");
        Thread.sleep(60000L);
    }

    private static Application invalidApplication(String name) throws Exception {
        Application app = new Application(name);
        ResourceProperties properties = new ResourceProperties();
        properties.setProperty("swarm.join.startupRetry", "false");
        properties.setProperty("swarm.join.minViewSize", "2");
        Field field = Application.class.getDeclaredField("props");
        field.setAccessible(true);
        field.set(app, properties);
        Field eventLog = Application.class.getDeclaredField("eventLog");
        eventLog.setAccessible(true);
        eventLog.set(app, LogFactory.getLog(BootstrapSigtermMain.class));
        return app;
    }

    private static void sessionWait() throws Exception {
        StartupShutdownHook hook = runtimeHook();
        final SessionStateStartupToken token = new SessionStateStartupToken();
        hook.register(token);
        InitialSessionStateSynchronizer synchronizer =
                new InitialSessionStateSynchronizer(
                        new InitialSessionStateSynchronizer.Transfer() {
                            public boolean request() {
                                return true;
                            }
                        }, new InitialSessionStateSynchronizer.ApplyAck() {
                            public void begin() {
                            }

                            public boolean isApplied() {
                                return false;
                            }

                            public boolean awaitApplied(long timeoutMillis)
                                    throws InterruptedException {
                                ready("session-wait");
                                while (true) {
                                    Thread.sleep(60000L);
                                }
                            }

                            public void end() {
                            }
                        }, new InitialSessionStateSynchronizer.SeedPolicy() {
                            public boolean maySeed() {
                                return false;
                            }
                        }, new InitialSessionStateSynchronizer.Delay() {
                            public long next(long capMillis) {
                                return capMillis;
                            }
                        }, token);
        synchronizer.synchronize();
    }

    private static void channelBootstrap(final String mode) throws Exception {
        final StartupShutdownHook hook = runtimeHook();
        final AtomicBoolean waitSignalled = new AtomicBoolean();
        BootstrapState state = new BootstrapState();
        hook.register(state);
        StartupChannelBootstrap bootstrap = new StartupChannelBootstrap(
                new StartupChannelBootstrap.Dependencies(
                        new StartupChannelBootstrap.ChannelFactory() {
                            public Channel create(Application ignored) {
                                TestChannel channel = new TestChannel(
                                        "view-wait".equals(mode) ? 1 : 2);
                                channel.failConnect = "backoff".equals(mode);
                                return channel;
                            }
                        }, new ProcessAdapterFactory(mode),
                        new StartupChannelBootstrap.Scheduler() {
                            public long nowMillis() {
                                return System.currentTimeMillis();
                            }

                            public void sleep(long millis)
                                    throws InterruptedException {
                                if (waitSignalled.compareAndSet(false, true)) {
                                    ready(mode);
                                }
                                Thread.sleep(millis);
                            }
                        }, new StartupChannelBootstrap.Jitter() {
                            public long delay(long capMillis) {
                                return capMillis;
                            }
                        }, new StartupChannelBootstrap.ClusterName() {
                            public String get(Application ignored) {
                                return "sigterm_swarm";
                            }
                        }));
        PullPushAdapter adapter = bootstrap.bootstrap(
                new Application("sigterm-" + mode), state,
                StartupJoinPolicy.parse(properties()));
        if ("published-cleanup".equals(mode)) {
            ready(mode);
            while (!hook.isShuttingDown()) {
                Thread.sleep(100L);
            }
        } else if (adapter != null) {
            throw new IllegalStateException("unexpected bootstrap completion");
        }
    }

    private static Properties properties() {
        Properties properties = new Properties();
        properties.setProperty("swarm.join.startupRetry", "true");
        properties.setProperty("swarm.join.minViewSize", "2");
        properties.setProperty("swarm.join.minViewWaitMillis", "60000");
        properties.setProperty("swarm.join.retryInitialDelayMillis", "60000");
        properties.setProperty("swarm.join.retryMaxDelayMillis", "60000");
        return properties;
    }

    private static void ready(String mode) {
        System.out.println("SIGTERM_READY " + mode);
        System.out.flush();
    }

    private static void awaitLeader(BootstrapState state) throws Exception {
        long deadline = System.currentTimeMillis() + 5000L;
        while (!state.hasLeader()) {
            if (System.currentTimeMillis() >= deadline) {
                throw new IllegalStateException("bootstrap leader did not enter fail-stop");
            }
            Thread.sleep(10L);
        }
    }

    private static final class ProcessAdapterFactory
            implements StartupChannelBootstrap.AdapterFactory {
        private final String mode;

        ProcessAdapterFactory(String mode) {
            this.mode = mode;
        }

        public PullPushAdapter create(Channel channel) {
            return new PullPushAdapter(channel, null, null, false);
        }

        public void start(PullPushAdapter adapter) throws Exception {
            if ("adapter-start".equals(mode)) {
                ready(mode);
                Thread.sleep(60000L);
            }
        }

        public void stop(PullPushAdapter adapter) {
            if ("published-cleanup".equals(mode)) {
                while (true) {
                    try {
                        Thread.sleep(60000L);
                    } catch (InterruptedException ignored) {
                    }
                }
            }
        }
    }

    private static final class TestChannel extends JChannel {
        private final View view;
        boolean failConnect;

        TestChannel(int memberCount) {
            super(false);
            Vector members = new Vector();
            for (int i = 0; i < memberCount; i++) {
                members.add(new IpAddress(17809 + i));
            }
            view = new View((Address) members.firstElement(), 1L, members);
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
            connected = false;
        }

        public synchronized void close() {
            connected = false;
            closed = true;
        }
    }

}
