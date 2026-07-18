package helma.swarm;

import org.jgroups.ChannelException;

final class InitialSessionStateSynchronizer {

    interface Transfer {
        boolean request() throws Exception;
    }

    interface ApplyAck {
        void begin();

        boolean isApplied();

        boolean awaitApplied(long timeoutMillis) throws InterruptedException;

        void end();
    }

    interface SeedPolicy {
        boolean maySeed();
    }

    interface Delay {
        long next(long capMillis);
    }

    private final Transfer transfer;
    private final ApplyAck ack;
    private final SeedPolicy seed;
    private final Delay delay;
    private final SessionStateStartupToken cancellation;

    InitialSessionStateSynchronizer(Transfer transfer, ApplyAck ack,
                                    SeedPolicy seed, Delay delay,
                                    SessionStateStartupToken cancellation) {
        this.transfer = transfer;
        this.ack = ack;
        this.seed = seed;
        this.delay = delay;
        this.cancellation = cancellation;
    }

    void synchronize() throws ChannelException {
        long cap = 1000L;
        while (!cancellation.isCancelled()) {
            boolean received = false;
            boolean applied = false;
            ack.begin();
            try {
                received = transfer.request();
                if (received) {
                    // JGroups 2.12.3 may return from getState() after queuing
                    // setState(), but before the listener has applied it. Do not
                    // start a second indistinguishable request while that callback
                    // is still pending; cancellation interrupts this wait.
                    applied = ack.awaitApplied(0L);
                }
            } catch (InterruptedException interrupted) {
                if (cancellation.isCancelled()) {
                    Thread.currentThread().interrupt();
                    break;
                }
                waitUntilCancelled();
                Thread.currentThread().interrupt();
                break;
            } catch (Exception ignored) {
            } finally {
                ack.end();
            }
            if (cancellation.isCancelled()) {
                break;
            }
            if (received && applied && ack.isApplied()) {
                return;
            }
            if (seed.maySeed()) {
                return;
            }
            try {
                cancellation.awaitDelay(delay.next(cap));
            } catch (InterruptedException interrupted) {
                if (!cancellation.isCancelled()) {
                    waitUntilCancelled();
                }
                Thread.currentThread().interrupt();
                break;
            }
            cap = cap >= 5000L ? 10000L : cap * 2L;
        }
        throw new ChannelException("initial session state synchronization cancelled");
    }

    private void waitUntilCancelled() {
        while (!cancellation.isCancelled()) {
            try {
                cancellation.awaitCancellation();
            } catch (InterruptedException ignored) {
            }
        }
    }
}
