/*
 * Helma License Notice
 *
 * The contents of this file are subject to the Helma License
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License.
 */
package helma.swarm;

import org.jgroups.Channel;
import org.jgroups.View;
import org.jgroups.blocks.PullPushAdapter;

public final class SwarmLifecycle {

    public static enum JoinStatus {
        NOT_STARTED, CONNECTING, PRE_READY, READY, CONFIG_ERROR, STOPPED
    }

    public static enum Capability {
        MANAGER_PENDING, NON_SESSION, SESSION_STARTING, SESSION_READY
    }

    private volatile SwarmJoinPolicy policy;
    private volatile JoinStatus joinStatus = JoinStatus.NOT_STARTED;
    private volatile Capability capability = Capability.MANAGER_PENDING;
    private volatile int attemptCount;
    private volatile String lastError = "";
    private volatile PullPushAdapter adapter;
    private volatile Channel candidate;
    private volatile SessionCapabilityService controlService;

    SwarmLifecycle(SwarmJoinPolicy policy) {
        this.policy = policy;
    }

    synchronized void setPolicy(SwarmJoinPolicy policy) {
        this.policy = policy;
    }

    synchronized int beginAttempt() {
        attemptCount++;
        joinStatus = JoinStatus.CONNECTING;
        lastError = "";
        return attemptCount;
    }

    synchronized void setCandidate(Channel channel) {
        if (joinStatus == JoinStatus.STOPPED) {
            closeChannel(channel);
            return;
        }
        candidate = channel;
    }

    synchronized void clearCandidate(Channel channel) {
        if (candidate == channel) {
            candidate = null;
        }
    }

    synchronized boolean publishPreReady(PullPushAdapter publishedAdapter) {
        if (joinStatus == JoinStatus.STOPPED) {
            return false;
        }
        adapter = publishedAdapter;
        candidate = null;
        joinStatus = JoinStatus.PRE_READY;
        lastError = "";
        return true;
    }

    synchronized void publishLegacyReady(PullPushAdapter publishedAdapter) {
        adapter = publishedAdapter;
        candidate = null;
        joinStatus = JoinStatus.READY;
        lastError = "";
    }

    public synchronized void commitReady() {
        if (joinStatus == JoinStatus.PRE_READY) {
            joinStatus = JoinStatus.READY;
            lastError = "";
        }
    }

    synchronized void attemptFailed(Throwable failure) {
        if (joinStatus != JoinStatus.STOPPED) {
            joinStatus = JoinStatus.CONNECTING;
            lastError = safeError(failure);
        }
    }

    public synchronized void configurationError(Throwable failure) {
        if (joinStatus != JoinStatus.STOPPED) {
            joinStatus = JoinStatus.CONFIG_ERROR;
            lastError = safeError(failure);
        }
    }

    public void setControlService(SessionCapabilityService service) {
        synchronized (this) {
            if (joinStatus != JoinStatus.STOPPED) {
                controlService = service;
                return;
            }
        }
        if (service != null) {
            service.stop();
        }
    }

    public synchronized void setCapability(Capability capability) {
        if (capability == null) {
            throw new NullPointerException("capability");
        }
        this.capability = capability;
    }

    public void stop() {
        SessionCapabilityService service;
        PullPushAdapter currentAdapter;
        Channel currentCandidate;
        synchronized (this) {
            if (joinStatus == JoinStatus.STOPPED) {
                return;
            }
            joinStatus = JoinStatus.STOPPED;
            capability = Capability.MANAGER_PENDING;
            service = controlService;
            controlService = null;
            currentAdapter = adapter;
            adapter = null;
            currentCandidate = candidate;
            candidate = null;
        }
        if (service != null) {
            service.stop();
        }
        if (currentAdapter != null) {
            try {
                currentAdapter.unregisterListener(ChannelUtils.SESSION_CONTROL);
            } catch (RuntimeException ignored) {
            }
            try {
                currentAdapter.stop();
            } catch (RuntimeException ignored) {
            }
            Channel transport = (Channel) currentAdapter.getTransport();
            closeChannel(transport);
            if (currentCandidate == transport) {
                currentCandidate = null;
            }
        }
        closeChannel(currentCandidate);
    }

    private static void closeChannel(Channel channel) {
        if (channel == null) {
            return;
        }
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
    }

    static String safeError(Throwable failure) {
        if (failure == null) {
            return "";
        }
        String message = failure.getMessage();
        if (message == null || message.length() == 0) {
            return failure.getClass().getName();
        }
        message = message.replaceAll("(?i)jdbc:[^\\s]+", "jdbc:<redacted>");
        message = message.replaceAll("(?i)(password|user)=[^&;\\s]+", "$1=<redacted>");
        return failure.getClass().getName() + ": " + message;
    }

    public SwarmJoinPolicy getPolicy() {
        return policy;
    }

    public SwarmJoinPolicy.MemberRole getMemberRole() {
        SwarmJoinPolicy current = policy;
        return current == null ? SwarmJoinPolicy.MemberRole.LEGACY
                : current.getMemberRole();
    }

    public JoinStatus getJoinStatus() {
        return joinStatus;
    }

    public JoinStatus getStatus() {
        return joinStatus;
    }

    public Capability getCapability() {
        return capability;
    }

    public int getAttemptCount() {
        return attemptCount;
    }

    public String getLastError() {
        return lastError;
    }

    public PullPushAdapter getAdapter() {
        return adapter;
    }

    public Channel getChannel() {
        PullPushAdapter current = adapter;
        if (current != null) {
            return (Channel) current.getTransport();
        }
        return candidate;
    }

    public View getView() {
        Channel channel = getChannel();
        return channel == null ? null : channel.getView();
    }

    public String getViewId() {
        View view = getView();
        return view == null || view.getViewId() == null ? "" : view.getViewId().toString();
    }

    public SessionCapabilityService getControlService() {
        return controlService;
    }
}
