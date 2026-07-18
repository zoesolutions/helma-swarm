package helma.swarm;

import helma.framework.core.Application;
import org.jgroups.Channel;
import org.jgroups.ChannelException;
import org.jgroups.blocks.PullPushAdapter;

import java.util.Properties;
import java.util.Enumeration;
import java.util.Vector;

final class StartupChannelBootstrap extends StartupBootstrapFactory {

    interface ChannelFactory {
        Channel create(Application app) throws Exception;
    }

    interface AdapterFactory {
        PullPushAdapter create(Channel channel) throws Exception;

        void start(PullPushAdapter adapter) throws Exception;

        void stop(PullPushAdapter adapter) throws Exception;
    }

    interface Scheduler {
        long nowMillis();

        void sleep(long millis) throws InterruptedException;
    }

    interface Jitter {
        long delay(long capMillis);
    }

    interface ClusterName {
        String get(Application app);
    }

    static final class Dependencies {
        final ChannelFactory channels;
        final AdapterFactory adapters;
        final Scheduler scheduler;
        final Jitter jitter;
        final ClusterName clusterName;

        Dependencies(ChannelFactory channels, AdapterFactory adapters,
                     Scheduler scheduler, Jitter jitter) {
            this(channels, adapters, scheduler, jitter, new ClusterName() {
                public String get(Application app) {
                    return app.getProperty("swarm.name", app.getName()) + "_swarm";
                }
            });
        }

        Dependencies(ChannelFactory channels, AdapterFactory adapters,
                     Scheduler scheduler, Jitter jitter,
                     ClusterName clusterName) {
            this.channels = channels;
            this.adapters = adapters;
            this.scheduler = scheduler;
            this.jitter = jitter;
            this.clusterName = clusterName;
        }
    }

    private final Dependencies dependencies;

    StartupChannelBootstrap(Dependencies dependencies) {
        this.dependencies = dependencies;
    }

    PullPushAdapter bootstrap(Application app, BootstrapState state,
                              StartupJoinPolicy policy) throws Exception {
        if (!state.claimLeader()) {
            try {
                return state.awaitAdapter();
            } catch (InterruptedException interrupted) {
                if (!state.isCancelled()) {
                    waitUntilCancelled(state);
                }
                Thread.currentThread().interrupt();
                throw new ChannelException(
                        "startup channel bootstrap interrupted");
            }
        }

        long retryCap = policy.getRetryInitialDelayMillis();
        try {
            while (!state.isCancelled()) {
                StartupCandidate candidate = null;
                try {
                    Channel channel = dependencies.channels.create(app);
                    candidate = new StartupCandidate(channel, dependencies.adapters);
                    if (!state.registerCandidate(candidate)) {
                        state.cleanupUnregistered(candidate);
                        throw cancelled();
                    }

                    channel.connect(dependencies.clusterName.get(app));
                    awaitMinimumView(channel, state, policy);

                    PullPushAdapter adapter = dependencies.adapters.create(channel);
                    if (!candidate.attachAdapter(adapter)) {
                        dependencies.adapters.stop(adapter);
                        throw cancelled();
                    }
                    if (!candidate.startAdapter()) {
                        throw cancelled();
                    }
                    if (!state.publish(candidate)) {
                        throw cancelled();
                    }
                    return adapter;
                } catch (StartupCancelledException cancelled) {
                    cleanup(state, candidate);
                    throw new ChannelException("startup channel bootstrap cancelled");
                } catch (InterruptedException interrupted) {
                    cleanup(state, candidate);
                    if (state.isCancelled()) {
                        Thread.currentThread().interrupt();
                        throw new ChannelException(
                                "startup channel bootstrap cancelled");
                    }
                    waitUntilCancelled(state);
                    Thread.currentThread().interrupt();
                    throw new ChannelException(
                            "startup channel bootstrap interrupted");
                } catch (Exception retryable) {
                    boolean cleaned = cleanup(state, candidate);
                    if (state.isCancelled()) {
                        throw new ChannelException(
                                "startup channel bootstrap cancelled");
                    }
                    if (!cleaned) {
                        waitUntilCancelled(state);
                        throw new ChannelException(
                                "startup channel cleanup failed");
                    }
                    try {
                        dependencies.scheduler.sleep(dependencies.jitter.delay(retryCap));
                    } catch (InterruptedException interrupted) {
                        if (state.isCancelled()) {
                            Thread.currentThread().interrupt();
                            throw new ChannelException(
                                    "startup channel bootstrap cancelled");
                        }
                        waitUntilCancelled(state);
                        Thread.currentThread().interrupt();
                        throw new ChannelException(
                                "startup channel bootstrap interrupted");
                    }
                    retryCap = doubledCap(retryCap,
                            policy.getRetryMaxDelayMillis());
                }
            }
            throw new ChannelException("startup channel bootstrap cancelled");
        } finally {
            state.leaderFinished();
        }
    }

    private void awaitMinimumView(Channel channel, BootstrapState state,
                                  StartupJoinPolicy policy)
            throws Exception {
        long deadline = safeAdd(dependencies.scheduler.nowMillis(),
                policy.getMinViewWaitMillis());
        while (viewSize(channel) < policy.getMinViewSize()) {
            if (state.isCancelled()) {
                throw cancelled();
            }
            long remaining = deadline - dependencies.scheduler.nowMillis();
            if (remaining <= 0L) {
                throw new ChannelException("minimum startup view was not reached");
            }
            dependencies.scheduler.sleep(Math.min(remaining, 100L));
        }
    }

    private static int viewSize(Channel channel) {
        org.jgroups.View view = channel.getView();
        Vector members = view == null ? null : view.getMembers();
        return members == null ? 0 : members.size();
    }

    private static long safeAdd(long left, long right) {
        return left > Long.MAX_VALUE - right ? Long.MAX_VALUE : left + right;
    }

    private static long doubledCap(long cap, long maximum) {
        return cap >= maximum / 2L ? maximum : cap * 2L;
    }

    static long equalJitterDelay(long capMillis, double sample) {
        if (capMillis < 0L || sample < 0.0d || sample >= 1.0d) {
            throw new IllegalArgumentException("invalid startup jitter input");
        }
        long lower = capMillis / 2L;
        long width = capMillis - lower;
        return lower + (long) (sample * (width + 1L));
    }

    private static boolean cleanup(BootstrapState state,
                                   StartupCandidate candidate) {
        StartupCandidate owned = state.takeCandidate(candidate);
        if (owned == null) {
            return true;
        }
        boolean cleaned = owned.cleanup();
        state.recordCleanupResult(cleaned);
        return cleaned;
    }

    private static void waitUntilCancelled(BootstrapState state) {
        boolean interrupted = false;
        while (!state.isCancelled()) {
            try {
                state.awaitCancellation();
            } catch (InterruptedException repeated) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private static StartupCancelledException cancelled() {
        return new StartupCancelledException();
    }

    private static final class StartupCancelledException extends Exception {
    }
}

abstract class StartupBootstrapFactory {
    abstract PullPushAdapter bootstrap(Application app, BootstrapState state,
                                       StartupJoinPolicy policy) throws Exception;
}

final class StartupJoinPolicy {

    private static final String ENABLED = "swarm.join.startupRetry";
    private static final String MIN_VIEW = "swarm.join.minViewSize";
    private static final String VIEW_WAIT = "swarm.join.minViewWaitMillis";
    private static final String INITIAL_DELAY = "swarm.join.retryInitialDelayMillis";
    private static final String MAX_DELAY = "swarm.join.retryMaxDelayMillis";
    private static final String[] STRICT_KEYS = new String[] {
        "swarm.join.strict",
        "swarm.join.dbSource",
        "swarm.join.validationQuery",
        "swarm.join.validationQueryTimeoutSeconds",
        "swarm.join.maxConnectTimeoutMillis",
        "swarm.join.maxAttempts"
    };

    private final boolean enabled;
    private final int minViewSize;
    private final long minViewWaitMillis;
    private final long retryInitialDelayMillis;
    private final long retryMaxDelayMillis;

    private StartupJoinPolicy(boolean enabled, int minViewSize,
                              long minViewWaitMillis,
                              long retryInitialDelayMillis,
                              long retryMaxDelayMillis) {
        this.enabled = enabled;
        this.minViewSize = minViewSize;
        this.minViewWaitMillis = minViewWaitMillis;
        this.retryInitialDelayMillis = retryInitialDelayMillis;
        this.retryMaxDelayMillis = retryMaxDelayMillis;
    }

    static StartupJoinPolicy parse(Properties properties) {
        if (properties == null) {
            throw new IllegalArgumentException("startup properties are required");
        }
        rejectStrictKeys(properties);
        boolean tunablesPresent = has(properties, MIN_VIEW)
                || has(properties, VIEW_WAIT)
                || has(properties, INITIAL_DELAY)
                || has(properties, MAX_DELAY);
        String rawEnabled = properties.getProperty(ENABLED);
        if (rawEnabled == null) {
            if (tunablesPresent) {
                throw invalid(ENABLED);
            }
            return disabled();
        }

        boolean enabled = parseBoolean(rawEnabled, ENABLED);
        if (!enabled) {
            if (tunablesPresent) {
                throw invalid(ENABLED);
            }
            return disabled();
        }

        int minView = parseInt(properties, MIN_VIEW, 2, 1, 32);
        long wait = parseLong(properties, VIEW_WAIT, 10000L, 100L, 60000L);
        long initial = parseLong(properties, INITIAL_DELAY, 1000L, 1000L, 60000L);
        long maximum = parseLong(properties, MAX_DELAY, 60000L, 60000L, 60000L);
        return new StartupJoinPolicy(true, minView, wait, initial, maximum);
    }

    boolean isEnabled() {
        return enabled;
    }

    int getMinViewSize() {
        return minViewSize;
    }

    long getMinViewWaitMillis() {
        return minViewWaitMillis;
    }

    long getRetryInitialDelayMillis() {
        return retryInitialDelayMillis;
    }

    long getRetryMaxDelayMillis() {
        return retryMaxDelayMillis;
    }

    private static StartupJoinPolicy disabled() {
        return new StartupJoinPolicy(false, 1, 0L, 0L, 0L);
    }

    private static boolean has(Properties properties, String name) {
        return properties.getProperty(name) != null;
    }

    private static boolean parseBoolean(String value, String name) {
        String canonical = value.trim();
        if ("true".equalsIgnoreCase(canonical)) {
            return true;
        }
        if ("false".equalsIgnoreCase(canonical)) {
            return false;
        }
        throw invalid(name);
    }

    private static int parseInt(Properties properties, String name,
                                int fallback, int minimum, int maximum) {
        return (int) parseLong(properties, name, fallback, minimum, maximum);
    }

    private static long parseLong(Properties properties, String name,
                                  long fallback, long minimum, long maximum) {
        String raw = properties.getProperty(name);
        if (raw == null) {
            return fallback;
        }
        final long parsed;
        try {
            parsed = Long.parseLong(raw.trim());
        } catch (RuntimeException failure) {
            throw invalid(name);
        }
        if (parsed < minimum || parsed > maximum) {
            throw invalid(name);
        }
        return parsed;
    }

    private static void rejectStrictKeys(Properties properties) {
        for (int i = 0; i < STRICT_KEYS.length; i++) {
            if (has(properties, STRICT_KEYS[i])) {
                throw invalid(STRICT_KEYS[i]);
            }
        }
        Enumeration names = properties.propertyNames();
        while (names.hasMoreElements()) {
            String name = String.valueOf(names.nextElement());
            if (name.startsWith("swarm.session.")) {
                throw invalid(name);
            }
        }
    }

    private static IllegalArgumentException invalid(String name) {
        return new IllegalArgumentException("invalid startup property: " + name);
    }
}

final class StartupCandidate {

    private final Channel channel;
    private final StartupChannelBootstrap.AdapterFactory adapters;
    private PullPushAdapter adapter;
    private boolean closed;
    private boolean cleanupSuccessful;

    StartupCandidate(Channel channel,
                     StartupChannelBootstrap.AdapterFactory adapters) {
        this.channel = channel;
        this.adapters = adapters;
    }

    synchronized boolean attachAdapter(PullPushAdapter value) {
        if (closed) {
            return false;
        }
        adapter = value;
        return true;
    }

    synchronized PullPushAdapter getAdapter() {
        return adapter;
    }

    synchronized boolean startAdapter() throws Exception {
        if (closed || adapter == null) {
            return false;
        }
        adapters.start(adapter);
        return true;
    }

    synchronized boolean cleanup() {
        if (closed) {
            return cleanupSuccessful;
        }
        closed = true;
        boolean successful = true;
        PullPushAdapter adapterToStop = adapter;
        adapter = null;
        if (adapterToStop != null) {
            try {
                adapters.stop(adapterToStop);
            } catch (Throwable ignored) {
                successful = false;
            }
        }
        try {
            if (channel.isConnected()) {
                channel.disconnect();
            }
        } catch (Throwable ignored) {
            successful = false;
        }
        try {
            if (channel.isOpen()) {
                channel.close();
            }
        } catch (Throwable ignored) {
            successful = false;
        }
        try {
            successful = !channel.isConnected() && !channel.isOpen() && successful;
        } catch (Throwable ignored) {
            successful = false;
        }
        cleanupSuccessful = successful;
        return cleanupSuccessful;
    }
}

final class BootstrapState implements StartupCancellation {

    interface CancellationListener {
        void cancelled(BootstrapState state);

        void finished(BootstrapState state);
    }

    interface PublicationAction {
        boolean publish(PullPushAdapter adapter);
    }

    private static final int BOOTSTRAPPING = 0;
    private static final int PUBLISHED = 1;
    private static final int CANCELLED = 2;
    private static final int STOPPED = 3;

    private int status = BOOTSTRAPPING;
    private boolean cancelled;
    private Thread leader;
    private StartupCandidate candidate;
    private StartupCandidate resource;
    private PullPushAdapter published;
    private boolean configurationFailureRecorded;
    private int cleanupInProgress;
    private boolean cleanupFailed;
    private boolean cancellationFinished;
    private boolean removalNotified;
    private final CancellationListener cancellationListener;
    private final Object publicationGate = new Object();

    BootstrapState() {
        this(null);
    }

    BootstrapState(CancellationListener cancellationListener) {
        this.cancellationListener = cancellationListener;
    }

    synchronized boolean claimLeader() {
        if (status != BOOTSTRAPPING || leader != null) {
            return false;
        }
        leader = Thread.currentThread();
        return true;
    }

    synchronized PullPushAdapter awaitAdapter()
            throws InterruptedException, ChannelException {
        while (status == BOOTSTRAPPING) {
            wait();
        }
        if (status == PUBLISHED) {
            return published;
        }
        throw new ChannelException("startup channel bootstrap cancelled");
    }

    synchronized boolean registerCandidate(StartupCandidate value) {
        if (status != BOOTSTRAPPING || leader != Thread.currentThread()
                || candidate != null) {
            return false;
        }
        candidate = value;
        return true;
    }

    synchronized StartupCandidate takeCandidate(StartupCandidate expected) {
        if (candidate == null || candidate != expected) {
            return null;
        }
        StartupCandidate owned = candidate;
        candidate = null;
        cleanupInProgress++;
        return owned;
    }

    boolean cleanupUnregistered(StartupCandidate value) {
        synchronized (this) {
            cleanupInProgress++;
        }
        boolean cleaned = value.cleanup();
        recordCleanupResult(cleaned);
        return cleaned;
    }

    synchronized boolean publish(StartupCandidate expected) {
        if (status != BOOTSTRAPPING || candidate != expected) {
            return false;
        }
        candidate = null;
        resource = expected;
        published = expected.getAdapter();
        status = PUBLISHED;
        notifyAll();
        return true;
    }

    boolean publishToRegistry(PublicationAction action) {
        synchronized (publicationGate) {
            PullPushAdapter adapter;
            synchronized (this) {
                if (status != PUBLISHED) {
                    return false;
                }
                adapter = published;
            }
            return action.publish(adapter);
        }
    }

    public void cancel() {
        Thread owner;
        StartupCandidate owned;
        synchronized (publicationGate) {
            synchronized (this) {
                if (status == CANCELLED || status == STOPPED) {
                    return;
                }
                cancelled = true;
                status = status == PUBLISHED ? STOPPED : CANCELLED;
                owner = leader;
                owned = candidate != null ? candidate : resource;
                candidate = null;
                resource = null;
                published = null;
                if (owned != null) {
                    cleanupInProgress++;
                }
                notifyAll();
            }
            if (cancellationListener != null) {
                cancellationListener.cancelled(this);
            }
        }
        if (owner != null && owner != Thread.currentThread()) {
            owner.interrupt();
        }
        if (owned != null) {
            recordCleanupResult(owned.cleanup());
        }
        cancellationFinished();
    }

    synchronized boolean isCancelled() {
        return cancelled;
    }

    synchronized boolean isPublished() {
        return status == PUBLISHED;
    }

    synchronized boolean hasLeader() {
        return leader != null;
    }

    synchronized boolean isAborted() {
        return status == CANCELLED || status == STOPPED;
    }

    synchronized boolean isCancellationComplete() {
        return isAborted() && cancellationFinished
                && cleanupInProgress == 0 && !cleanupFailed && leader == null;
    }

    synchronized boolean recordConfigurationFailure() {
        if (configurationFailureRecorded) {
            return false;
        }
        configurationFailureRecorded = true;
        return true;
    }

    void recordCleanupResult(boolean successful) {
        boolean finished;
        synchronized (this) {
            if (cleanupInProgress <= 0) {
                throw new IllegalStateException("no startup cleanup is active");
            }
            cleanupInProgress--;
            if (!successful) {
                cleanupFailed = true;
            }
            finished = markRemovalIfComplete();
        }
        if (finished && cancellationListener != null) {
            cancellationListener.finished(this);
        }
    }

    synchronized void awaitCancellation() throws InterruptedException {
        if (!cancelled) {
            wait();
        }
    }

    void leaderFinished() {
        boolean finished = false;
        synchronized (this) {
            if (leader == Thread.currentThread()) {
                leader = null;
                notifyAll();
                finished = markRemovalIfComplete();
            }
        }
        if (finished && cancellationListener != null) {
            cancellationListener.finished(this);
        }
    }

    private void cancellationFinished() {
        boolean finished;
        synchronized (this) {
            cancellationFinished = true;
            finished = markRemovalIfComplete();
        }
        if (finished && cancellationListener != null) {
            cancellationListener.finished(this);
        }
    }

    private boolean markRemovalIfComplete() {
        if (isAborted() && cancellationFinished && cleanupInProgress == 0
                && !cleanupFailed && leader == null && !removalNotified) {
            removalNotified = true;
            return true;
        }
        return false;
    }
}

final class SessionStateStartupToken implements StartupCancellation {

    private final Thread owner;
    private final Runnable cleanup;
    private boolean cancelled;

    SessionStateStartupToken() {
        this(null);
    }

    SessionStateStartupToken(Runnable cleanup) {
        this.owner = Thread.currentThread();
        this.cleanup = cleanup;
    }

    public void cancel() {
        synchronized (this) {
            if (cancelled) {
                return;
            }
            cancelled = true;
            notifyAll();
        }
        if (owner != Thread.currentThread()) {
            owner.interrupt();
        }
        if (cleanup != null) {
            cleanup.run();
        }
    }

    synchronized boolean isCancelled() {
        return cancelled;
    }

    synchronized void awaitDelay(long millis) throws InterruptedException {
        if (!cancelled) {
            wait(millis);
        }
    }

    synchronized void awaitCancellation() throws InterruptedException {
        if (!cancelled) {
            wait();
        }
    }
}
