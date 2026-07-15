/*
 * Helma License Notice
 *
 * The contents of this file are subject to the Helma License
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License.
 */
package helma.swarm;

import helma.framework.core.Application;
import helma.objectmodel.db.DbSource;
import org.jgroups.Channel;
import org.jgroups.ChannelException;
import org.jgroups.JChannel;
import org.jgroups.View;
import org.jgroups.blocks.PullPushAdapter;

import java.sql.Connection;
import java.sql.Statement;

interface DatabaseProbe {
    void validate(Application app, SwarmJoinPolicy policy) throws Exception;
}

interface ChannelFactory {
    Channel create(Application app) throws Exception;
}

interface Sleeper {
    void sleep(long delayMillis) throws InterruptedException;
}

final class SwarmChannelBootstrap {

    private final Application app;
    private final SwarmLifecycle lifecycle;
    private final DatabaseProbe databaseProbe;
    private final ChannelFactory channelFactory;
    private final Sleeper sleeper;
    private final ProcessShutdown shutdown;

    SwarmChannelBootstrap(Application app, SwarmLifecycle lifecycle) {
        this(app, lifecycle, new DefaultDatabaseProbe(), new DefaultChannelFactory(),
                new ThreadSleeper(), ProcessShutdown.current());
    }

    SwarmChannelBootstrap(Application app, SwarmLifecycle lifecycle,
                          DatabaseProbe databaseProbe, ChannelFactory channelFactory,
                          Sleeper sleeper, ProcessShutdown shutdown) {
        this.app = app;
        this.lifecycle = lifecycle;
        this.databaseProbe = databaseProbe;
        this.channelFactory = channelFactory;
        this.sleeper = sleeper;
        this.shutdown = shutdown;
    }

    PullPushAdapter bootstrap() throws ChannelException {
        int joinAttempts = 0;
        long retryDelay = 1000L;
        while (!shutdown.isShuttingDown()) {
            SwarmJoinPolicy policy;
            try {
                policy = SwarmJoinPolicy.from(app.getProperties());
                if (!policy.isStrict()) {
                    throw new IllegalArgumentException(
                            "swarm.join.strict changed while STRICT bootstrap was active");
                }
                lifecycle.setPolicy(policy);
                retryDelay = boundedDelay(retryDelay, policy);
            } catch (IllegalArgumentException invalidConfiguration) {
                lifecycle.beginAttempt();
                lifecycle.configurationError(invalidConfiguration);
                logFailure();
                sleepWithoutEscape(retryDelay);
                retryDelay = doubleDelay(retryDelay, 5000L);
                continue;
            }

            if (policy.getMaxAttempts() > 0
                    && joinAttempts >= policy.getMaxAttempts()) {
                sleepWithoutEscape(policy.getRetryMaxDelayMillis());
                continue;
            }

            joinAttempts++;
            lifecycle.beginAttempt();
            Channel channel = null;
            PullPushAdapter adapter = null;
            SessionCapabilityService controlService = null;
            try {
                databaseProbe.validate(app, policy);
                channel = channelFactory.create(app);
                lifecycle.setCandidate(channel);
                String groupName = app.getProperty("swarm.name", app.getName());
                channel.connect(groupName + "_swarm");
                waitForMinimumView(channel, policy);
                adapter = new PullPushAdapter(channel, null, null, false);
                controlService = new SessionCapabilityService(app, adapter, lifecycle);
                lifecycle.setControlService(controlService);
                adapter.registerListener(ChannelUtils.SESSION_CONTROL, controlService);
                adapter.start();
                if (shutdown.isShuttingDown()) {
                    closeAttempt(adapter, channel, controlService);
                    break;
                }
                return adapter;
            } catch (InterruptedException interrupted) {
                Thread.interrupted();
                lifecycle.attemptFailed(interrupted);
                closeAttempt(adapter, channel, controlService);
                if (shutdown.isShuttingDown()) {
                    break;
                }
                logFailure();
                sleepWithoutEscape(retryDelay);
                retryDelay = doubleDelay(retryDelay,
                        policy.getRetryMaxDelayMillis());
                continue;
            } catch (IllegalArgumentException invalidConfiguration) {
                lifecycle.configurationError(invalidConfiguration);
                closeAttempt(adapter, channel, controlService);
                logFailure();
                sleepWithoutEscape(retryDelay);
                retryDelay = doubleDelay(retryDelay,
                        policy.getRetryMaxDelayMillis());
                continue;
            } catch (Exception failure) {
                lifecycle.attemptFailed(failure);
                closeAttempt(adapter, channel, controlService);
                logFailure();
                sleepWithoutEscape(retryDelay);
                retryDelay = doubleDelay(retryDelay,
                        policy.getRetryMaxDelayMillis());
                continue;
            }
        }
        lifecycle.stop();
        throw new ChannelException("HelmaSwarm bootstrap stopped for JVM shutdown");
    }

    private void waitForMinimumView(Channel channel, SwarmJoinPolicy policy)
            throws Exception {
        long timeout = policy.getMinViewWaitMillis();
        long deadline = System.currentTimeMillis() + timeout;
        while (true) {
            View view = channel.getView();
            if (view != null && view.getMembers().size() >= policy.getMinViewSize()) {
                return;
            }
            long remaining = deadline - System.currentTimeMillis();
            if (remaining <= 0L) {
                throw new ChannelException("minimum swarm view was not reached before timeout");
            }
            sleeper.sleep(Math.min(50L, remaining));
        }
    }

    private void closeAttempt(PullPushAdapter adapter, Channel channel,
                              SessionCapabilityService controlService) {
        if (controlService != null) {
            controlService.stop();
        }
        if (adapter != null) {
            try {
                adapter.unregisterListener(ChannelUtils.SESSION_CONTROL);
            } catch (RuntimeException ignored) {
            }
            try {
                adapter.stop();
            } catch (RuntimeException ignored) {
            }
        }
        if (channel != null) {
            try {
                if (channel.isConnected()) {
                    channel.disconnect();
                }
            } catch (RuntimeException ignored) {
            }
            try {
                if (channel.isOpen()) {
                    channel.close();
                }
            } catch (RuntimeException ignored) {
            }
            lifecycle.clearCandidate(channel);
        }
        lifecycle.setControlService(null);
    }

    private void sleepWithoutEscape(long delayMillis) {
        long deadline = System.currentTimeMillis() + Math.max(0L, delayMillis);
        while (!shutdown.isShuttingDown()) {
            long remaining = deadline - System.currentTimeMillis();
            if (remaining <= 0L) {
                return;
            }
            try {
                sleeper.sleep(remaining);
            } catch (InterruptedException interrupted) {
                Thread.interrupted();
                lifecycle.attemptFailed(interrupted);
            }
        }
    }

    private void logFailure() {
        app.logEvent("HelmaSwarm: STRICT join attempt "
                + lifecycle.getAttemptCount() + " not ready: " + lifecycle.getLastError());
    }

    private static long boundedDelay(long delay, SwarmJoinPolicy policy) {
        return Math.max(policy.getRetryInitialDelayMillis(),
                Math.min(delay, policy.getRetryMaxDelayMillis()));
    }

    private static long doubleDelay(long delay, long maximum) {
        if (delay >= maximum) {
            return maximum;
        }
        if (delay > Long.MAX_VALUE / 2L) {
            return maximum;
        }
        return Math.min(delay * 2L, maximum);
    }

    private static final class DefaultDatabaseProbe implements DatabaseProbe {
        public void validate(Application app, SwarmJoinPolicy policy) throws Exception {
            String url = app.getDbProperties().getProperty(
                    policy.getDbSource() + ".url");
            SwarmJoinPolicy.validateMysqlConnectTimeout(url,
                    policy.getMaxConnectTimeoutMillis());
            DbSource source = app.getDbSource(policy.getDbSource());
            if (source == null) {
                throw new IllegalArgumentException("configured database source is unavailable");
            }
            Connection connection = null;
            Statement statement = null;
            try {
                connection = source.getConnection();
                statement = connection.createStatement();
                statement.setQueryTimeout(policy.getValidationQueryTimeoutSeconds());
                statement.execute(policy.getValidationQuery());
            } finally {
                if (statement != null) {
                    try {
                        statement.close();
                    } catch (Exception ignored) {
                    }
                }
                if (connection != null) {
                    try {
                        connection.close();
                    } catch (Exception ignored) {
                    }
                }
            }
        }
    }

    private static final class DefaultChannelFactory implements ChannelFactory {
        public Channel create(Application app) throws Exception {
            SwarmConfig config = new SwarmConfig(app);
            return new JChannel(config.getJGroupsProps());
        }
    }

    private static final class ThreadSleeper implements Sleeper {
        public void sleep(long delayMillis) throws InterruptedException {
            Thread.sleep(delayMillis);
        }
    }
}
