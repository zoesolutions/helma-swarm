package helma.swarm;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

interface StartupCancellation {
    void cancel();
}

final class StartupShutdownHook {

    interface HookInstaller {
        void addShutdownHook(Thread hook);
    }

    interface TimeoutReporter {
        void deadlineExceeded();
    }

    private final HookInstaller installer;
    private final long deadlineMillis;
    private final TimeoutReporter timeoutReporter;
    private final Object lock = new Object();
    private final Map states = new IdentityHashMap();
    private volatile boolean shuttingDown;
    private boolean installed;

    StartupShutdownHook(HookInstaller installer) {
        this(installer, 45000L, new TimeoutReporter() {
            public void deadlineExceeded() {
                System.err.println(
                        "HelmaSwarm: startup cleanup deadline exceeded");
            }
        });
    }

    StartupShutdownHook(HookInstaller installer, long deadlineMillis) {
        this(installer, deadlineMillis, new TimeoutReporter() {
            public void deadlineExceeded() {
                System.err.println(
                        "HelmaSwarm: startup cleanup deadline exceeded");
            }
        });
    }

    StartupShutdownHook(HookInstaller installer, long deadlineMillis,
                        TimeoutReporter timeoutReporter) {
        this.installer = installer;
        this.deadlineMillis = deadlineMillis;
        this.timeoutReporter = timeoutReporter;
    }

    void register(StartupCancellation state) {
        boolean cancel = false;
        boolean signal = false;
        synchronized (lock) {
            if (shuttingDown) {
                cancel = true;
            } else {
                if (!installed) {
                    try {
                        installer.addShutdownHook(new Thread(new Runnable() {
                            public void run() {
                                signalShutdown();
                            }
                        }, "helmaswarm-startup-shutdown"));
                        installed = true;
                    } catch (IllegalStateException shutdownInProgress) {
                        shuttingDown = true;
                        states.put(state, Integer.valueOf(1));
                        signal = true;
                    }
                }
                if (!cancel && !signal) {
                    Integer count = (Integer) states.get(state);
                    states.put(state, Integer.valueOf(
                            count == null ? 1 : count.intValue() + 1));
                }
            }
        }
        if (signal) {
            signalShutdown();
        } else if (cancel) {
            state.cancel();
        }
    }

    void deregister(StartupCancellation state) {
        synchronized (lock) {
            Integer count = (Integer) states.get(state);
            if (count == null || count.intValue() <= 1) {
                states.remove(state);
            } else {
                states.put(state, Integer.valueOf(count.intValue() - 1));
            }
        }
    }

    void signalShutdown() {
        List snapshot;
        synchronized (lock) {
            shuttingDown = true;
            snapshot = new ArrayList(states.keySet());
            states.clear();
        }
        List workers = new ArrayList();
        for (int i = 0; i < snapshot.size(); i++) {
            final StartupCancellation cancellation =
                    (StartupCancellation) snapshot.get(i);
            Thread worker = new Thread(new Runnable() {
                public void run() {
                    cancellation.cancel();
                }
            }, "helmaswarm-startup-cleanup-" + i);
            worker.setDaemon(true);
            workers.add(worker);
            worker.start();
        }
        long deadline = System.currentTimeMillis() + deadlineMillis;
        boolean timedOut = false;
        for (int i = 0; i < workers.size(); i++) {
            Thread worker = (Thread) workers.get(i);
            long remaining = deadline - System.currentTimeMillis();
            if (remaining <= 0L) {
                worker.interrupt();
                timedOut = true;
                continue;
            }
            try {
                worker.join(remaining);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return;
            }
            if (worker.isAlive()) {
                worker.interrupt();
                timedOut = true;
            }
        }
        if (timedOut) {
            timeoutReporter.deadlineExceeded();
        }
    }

    boolean isShuttingDown() {
        return shuttingDown;
    }
}
