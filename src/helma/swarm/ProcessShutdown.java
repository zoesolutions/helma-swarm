/*
 * Helma License Notice
 *
 * The contents of this file are subject to the Helma License
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License.
 */
package helma.swarm;

import java.util.ArrayList;
import java.util.List;

public final class ProcessShutdown {

    private static final ProcessShutdown INSTANCE = new ProcessShutdown(true);

    private final List lifecycles = new ArrayList();
    private volatile boolean shuttingDown;

    ProcessShutdown(boolean installHook) {
        if (installHook) {
            Thread hook = new Thread(new Runnable() {
                public void run() {
                    signalShutdown();
                }
            }, "HelmaSwarm-JVM-Shutdown");
            try {
                Runtime.getRuntime().addShutdownHook(hook);
            } catch (IllegalStateException alreadyShuttingDown) {
                signalShutdown();
            }
        }
    }

    public static ProcessShutdown current() {
        return INSTANCE;
    }

    synchronized void register(SwarmLifecycle lifecycle) {
        if (shuttingDown) {
            lifecycle.stop();
        } else if (!lifecycles.contains(lifecycle)) {
            lifecycles.add(lifecycle);
        }
    }

    synchronized void unregister(SwarmLifecycle lifecycle) {
        lifecycles.remove(lifecycle);
    }

    void signalShutdown() {
        Object[] snapshot;
        synchronized (this) {
            if (shuttingDown) {
                return;
            }
            shuttingDown = true;
            snapshot = lifecycles.toArray();
            lifecycles.clear();
            notifyAll();
        }
        for (int i = 0; i < snapshot.length; i++) {
            SwarmLifecycle lifecycle = (SwarmLifecycle) snapshot[i];
            lifecycle.stop();
        }
    }

    public boolean isShuttingDown() {
        return shuttingDown;
    }
}
