package helma.swarm;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import helma.framework.core.Application;
import org.jgroups.Address;
import org.jgroups.Channel;
import org.jgroups.Message;
import org.jgroups.MessageListener;
import org.jgroups.View;
import org.jgroups.blocks.PullPushAdapter;

final class SessionCapabilityService implements MessageListener {

    static final Integer PROTOCOL_VERSION = Integer.valueOf(1);
    static final String CAPABILITY_REQUEST = "CAPABILITY_REQUEST";
    static final String CAPABILITY_RESPONSE = "CAPABILITY_RESPONSE";
    static final String STATE_REQUEST = "STATE_REQUEST";

    private static final String MANAGER_PENDING = "MANAGER_PENDING";
    private static final String NON_SESSION = "NON_SESSION";
    private static final String SESSION_STARTING = "SESSION_STARTING";
    private static final String SESSION_READY = "SESSION_READY";

    private final PullPushAdapter adapter;
    private final SwarmLifecycle lifecycle;
    private final Object roundLock = new Object();

    private volatile SwarmSessionManager sessionManager;
    private volatile CapabilityRound capabilityRound;
    private volatile StateRound stateRound;
    private volatile CapabilityView lastCompleteView;
    private volatile boolean protocolComplete;
    private volatile boolean stopped;

    SessionCapabilityService(Application app, PullPushAdapter adapter,
                             SwarmLifecycle lifecycle) {
        if (app == null) {
            throw new IllegalArgumentException("app is required");
        }
        if (adapter == null) {
            throw new IllegalArgumentException("adapter is required");
        }
        if (lifecycle == null) {
            throw new IllegalArgumentException("lifecycle is required");
        }
        this.adapter = adapter;
        this.lifecycle = lifecycle;
    }

    void attachSessionManager(SwarmSessionManager manager) {
        sessionManager = manager;
    }

    CapabilityView discover(long timeout) throws InterruptedException {
        requireTimeout(timeout);
        View snapshot = currentView();
        Address local = localAddress();
        if (snapshot == null || local == null || !snapshot.containsMember(local)) {
            protocolComplete = false;
            return null;
        }

        String nonce = UUID.randomUUID().toString();
        String viewId = viewId(snapshot);
        CapabilityRound round = new CapabilityRound(nonce, viewId,
                new ArrayList<Address>(snapshot.getMembers()), local, getCapability());
        synchronized (roundLock) {
            if (stopped) {
                return null;
            }
            capabilityRound = round;
        }
        try {
            Hashtable request = controlMessage(CAPABILITY_REQUEST, nonce, viewId,
                    local.toString());
            for (Address member : remoteMembers(round.getMembers(), local)) {
                adapter.send(ChannelUtils.SESSION_CONTROL,
                        new Message(member, local, request));
            }
            await(round, timeout);
            CapabilityView complete = round.complete(currentViewId());
            protocolComplete = complete != null;
            if (complete != null) {
                lastCompleteView = complete;
            }
            return complete;
        } catch (InterruptedException interrupted) {
            protocolComplete = false;
            throw interrupted;
        } catch (Exception failure) {
            protocolComplete = false;
            return null;
        } finally {
            synchronized (roundLock) {
                if (capabilityRound == round) {
                    capabilityRound = null;
                }
            }
        }
    }

    Hashtable requestState(Address provider, long timeout) throws InterruptedException {
        requireTimeout(timeout);
        Address local = localAddress();
        CapabilityView completeView = lastCompleteView;
        if (provider == null || local == null || completeView == null
                || !protocolComplete || !completeView.getReadyProviders().contains(provider)
                || !completeView.getViewId().equals(currentViewId())) {
            return null;
        }

        String nonce = UUID.randomUUID().toString();
        String viewId = completeView.getViewId();
        SwarmJoinPolicy policy = lifecycle.getPolicy();
        StateRound round = new StateRound(nonce, viewId, provider,
                policy.getStateMaxBytes(), policy.getStateMaxEntries());
        synchronized (roundLock) {
            if (stopped) {
                return null;
            }
            stateRound = round;
        }
        try {
            Hashtable request = controlMessage(STATE_REQUEST, nonce, viewId,
                    provider.toString());
            adapter.send(ChannelUtils.SESSION_CONTROL,
                    new Message(provider, local, request));
            await(round, timeout);
            return round.complete(currentViewId());
        } catch (InterruptedException interrupted) {
            throw interrupted;
        } catch (Exception failure) {
            return null;
        } finally {
            synchronized (roundLock) {
                if (stateRound == round) {
                    stateRound = null;
                }
            }
        }
    }

    public void receive(Message message) {
        if (message == null || stopped) {
            return;
        }
        Address source = message.getSrc();
        Object payload;
        try {
            payload = message.getObject();
        } catch (RuntimeException malformed) {
            invalidateActiveRounds();
            return;
        }
        if (!(payload instanceof Hashtable)) {
            invalidateActiveRounds();
            return;
        }

        Hashtable control = (Hashtable) payload;
        Object type = control.get("type");
        if (CAPABILITY_REQUEST.equals(type)) {
            respondCapability(source, control);
        } else if (CAPABILITY_RESPONSE.equals(type)) {
            CapabilityRound round = capabilityRound;
            if (round != null) {
                round.accept(source, control);
                signalWaiters();
            }
        } else if (STATE_REQUEST.equals(type)) {
            respondState(source, control);
        } else if (SwarmSessionEnvelope.TYPE.equals(type)) {
            StateRound round = stateRound;
            if (round != null) {
                round.accept(source, control);
                signalWaiters();
            }
        } else {
            invalidateActiveRounds();
        }
    }

    public byte[] getState() {
        return null;
    }

    public void setState(byte[] state) {
    }

    void stop() {
        stopped = true;
        protocolComplete = false;
        synchronized (roundLock) {
            if (capabilityRound != null) {
                capabilityRound.invalidate();
            }
            if (stateRound != null) {
                stateRound.invalidate();
            }
            roundLock.notifyAll();
        }
        sessionManager = null;
    }

    String getCapability() {
        SwarmLifecycle.Capability capability = lifecycle.getCapability();
        return capability == null ? MANAGER_PENDING : capability.name();
    }

    boolean isProtocolComplete() {
        CapabilityView view = lastCompleteView;
        return protocolComplete && view != null && view.getViewId().equals(currentViewId());
    }

    boolean isControlProtocolComplete() {
        return isProtocolComplete();
    }

    CapabilityView getLastCompleteView() {
        return lastCompleteView;
    }

    List<Address> getKnownProviders() {
        CapabilityView view = lastCompleteView;
        return view == null ? Collections.<Address>emptyList() : view.getReadyProviders();
    }

    private void respondCapability(Address source, Hashtable request) {
        Address local = localAddress();
        String viewId = currentViewId();
        if (!validRequest(request, CAPABILITY_REQUEST, source, source, viewId)
                || local == null) {
            return;
        }
        Hashtable response = capabilityResponse((String) request.get("nonce"), viewId,
                local, getCapability());
        send(source, local, response);
    }

    private void respondState(Address source, Hashtable request) {
        Address local = localAddress();
        String viewId = currentViewId();
        SwarmSessionManager manager = sessionManager;
        if (local == null || manager == null || !SESSION_READY.equals(getCapability())
                || !validRequest(request, STATE_REQUEST, source, local, viewId)) {
            return;
        }
        try {
            String nonce = (String) request.get("nonce");
            Hashtable envelope = manager.createStateEnvelope(nonce, viewId, local.toString());
            SwarmJoinPolicy policy = lifecycle.getPolicy();
            SwarmSessionEnvelope.validate(envelope, nonce, viewId, local, local,
                    policy.getStateMaxBytes(), policy.getStateMaxEntries());
            send(source, local, envelope);
        } catch (RuntimeException incomplete) {
            return;
        }
    }

    private boolean validRequest(Hashtable request, String expectedType, Address source,
                                 Address provider, String expectedViewId) {
        if (source == null || provider == null || expectedViewId == null) {
            return false;
        }
        View view = currentView();
        if (view == null || !view.containsMember(source)) {
            return false;
        }
        try {
            requireControl(request, expectedType, (String) request.get("nonce"),
                    expectedViewId, provider.toString());
            return true;
        } catch (RuntimeException malformed) {
            return false;
        }
    }

    private void send(Address destination, Address local, Hashtable payload) {
        try {
            adapter.send(ChannelUtils.SESSION_CONTROL,
                    new Message(destination, local, payload));
        } catch (Exception ignored) {
            return;
        }
    }

    private void await(PendingRound round, long timeout) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeout;
        synchronized (roundLock) {
            while (!stopped && !round.isTerminal()) {
                long remaining = deadline - System.currentTimeMillis();
                if (remaining <= 0) {
                    return;
                }
                roundLock.wait(Math.min(remaining, 50L));
                if (!round.getViewId().equals(currentViewId())) {
                    round.invalidate();
                }
            }
        }
    }

    private void signalWaiters() {
        synchronized (roundLock) {
            roundLock.notifyAll();
        }
    }

    private void invalidateActiveRounds() {
        CapabilityRound capabilities = capabilityRound;
        StateRound state = stateRound;
        if (capabilities != null) {
            capabilities.invalidate();
        }
        if (state != null) {
            state.invalidate();
        }
        signalWaiters();
    }

    private Channel channel() {
        return lifecycle.getChannel();
    }

    private View currentView() {
        Channel channel = channel();
        return channel == null ? null : channel.getView();
    }

    private Address localAddress() {
        Channel channel = channel();
        return channel == null ? null : channel.getLocalAddress();
    }

    private String currentViewId() {
        View view = currentView();
        return view == null ? null : viewId(view);
    }

    private static String viewId(View view) {
        return view.getViewId().toString();
    }

    private static void requireTimeout(long timeout) {
        if (timeout < 0) {
            throw new IllegalArgumentException("timeout is negative");
        }
    }

    static Hashtable capabilityResponse(String nonce, String viewId, Address source,
                                        String capability) {
        requireCapability(capability);
        if (source == null) {
            throw new IllegalArgumentException("source is required");
        }
        Hashtable response = controlMessage(CAPABILITY_RESPONSE, nonce, viewId,
                source.toString());
        response.put("capability", capability);
        return response;
    }

    private static Hashtable controlMessage(String type, String nonce, String viewId,
                                            String provider) {
        requireText(type, "type");
        requireText(nonce, "nonce");
        requireText(viewId, "viewId");
        requireText(provider, "provider");
        Hashtable message = new Hashtable();
        message.put("protocol", PROTOCOL_VERSION);
        message.put("type", type);
        message.put("nonce", nonce);
        message.put("viewId", viewId);
        message.put("provider", provider);
        return message;
    }

    private static void requireControl(Hashtable message, String type, String nonce,
                                       String viewId, String provider) {
        if (!PROTOCOL_VERSION.equals(message.get("protocol"))) {
            throw new IllegalArgumentException("protocol mismatch");
        }
        requireEquals(type, requireText(message.get("type"), "type"), "type");
        requireEquals(nonce, requireText(message.get("nonce"), "nonce"), "nonce");
        requireEquals(viewId, requireText(message.get("viewId"), "viewId"), "viewId");
        requireEquals(provider, requireText(message.get("provider"), "provider"), "provider");
    }

    private static String requireText(Object value, String field) {
        if (!(value instanceof String) || ((String) value).length() == 0) {
            throw new IllegalArgumentException(field + " is empty");
        }
        return (String) value;
    }

    private static void requireText(String value, String field) {
        requireText((Object) value, field);
    }

    private static void requireEquals(String expected, String actual, String field) {
        if (expected == null || !expected.equals(actual)) {
            throw new IllegalArgumentException(field + " mismatch");
        }
    }

    private static void requireCapability(String capability) {
        if (!MANAGER_PENDING.equals(capability) && !NON_SESSION.equals(capability)
                && !SESSION_STARTING.equals(capability) && !SESSION_READY.equals(capability)) {
            throw new IllegalArgumentException("unknown capability");
        }
    }

    static List<Address> remoteMembers(List<Address> members, Address local) {
        if (members == null || local == null) {
            throw new IllegalArgumentException("view and local member are required");
        }
        ArrayList<Address> remote = new ArrayList<Address>();
        for (Address member : members) {
            if (!local.equals(member)) {
                remote.add(member);
            }
        }
        return Collections.unmodifiableList(remote);
    }

    interface PendingRound {
        boolean isTerminal();

        void invalidate();

        String getViewId();
    }

    static final class CapabilityRound implements PendingRound {
        private final String nonce;
        private final String viewId;
        private final List<Address> members;
        private final Address local;
        private final Map<Address, String> capabilities = new HashMap<Address, String>();
        private boolean invalid;

        CapabilityRound(String nonce, String viewId, List<Address> members, Address local,
                        String localCapability) {
            requireText(nonce, "nonce");
            requireText(viewId, "viewId");
            requireCapability(localCapability);
            if (members == null || local == null || !members.contains(local)) {
                throw new IllegalArgumentException("local member is absent from view");
            }
            this.nonce = nonce;
            this.viewId = viewId;
            this.members = Collections.unmodifiableList(new ArrayList<Address>(members));
            this.local = local;
            capabilities.put(local, localCapability);
        }

        synchronized void accept(Address source, Object payload) {
            if (invalid || source == null || source.equals(local) || !members.contains(source)
                    || !(payload instanceof Hashtable)) {
                invalid = true;
                return;
            }
            Hashtable response = (Hashtable) payload;
            if (!isCorrelated(response, CAPABILITY_RESPONSE, nonce, viewId,
                    source.toString())) {
                return;
            }
            try {
                requireControl(response, CAPABILITY_RESPONSE, nonce, viewId, source.toString());
                String capability = requireText(response.get("capability"), "capability");
                requireCapability(capability);
                String previous = capabilities.put(source, capability);
                if (previous != null && !previous.equals(capability)) {
                    invalid = true;
                }
            } catch (RuntimeException malformed) {
                invalid = true;
            }
        }

        synchronized CapabilityView complete(String currentViewId) {
            if (invalid || !viewId.equals(currentViewId)
                    || capabilities.size() != members.size()) {
                return null;
            }
            return new CapabilityView(viewId, members, capabilities);
        }

        public synchronized boolean isTerminal() {
            return invalid || capabilities.size() == members.size();
        }

        public synchronized void invalidate() {
            invalid = true;
        }

        public String getViewId() {
            return viewId;
        }

        List<Address> getMembers() {
            return members;
        }
    }

    static final class CapabilityView {
        private final String viewId;
        private final Map<Address, String> capabilities;
        private final List<Address> readyProviders;
        private final Address provider;
        private final Address seed;

        CapabilityView(String viewId, List<Address> members,
                       Map<Address, String> responseCapabilities) {
            this.viewId = viewId;
            LinkedHashMap<Address, String> ordered = new LinkedHashMap<Address, String>();
            ArrayList<Address> ready = new ArrayList<Address>();
            Address firstStarting = null;
            for (Address member : members) {
                String capability = responseCapabilities.get(member);
                ordered.put(member, capability);
                if (SESSION_READY.equals(capability)) {
                    ready.add(member);
                } else if (firstStarting == null && SESSION_STARTING.equals(capability)) {
                    firstStarting = member;
                }
            }
            capabilities = Collections.unmodifiableMap(ordered);
            readyProviders = Collections.unmodifiableList(ready);
            provider = ready.isEmpty() ? null : ready.get(0);
            seed = ready.isEmpty() ? firstStarting : null;
        }

        String getViewId() {
            return viewId;
        }

        Map<Address, String> getCapabilities() {
            return capabilities;
        }

        List<Address> getReadyProviders() {
            return readyProviders;
        }

        Address getProvider() {
            return provider;
        }

        Address getSeed() {
            return seed;
        }
    }

    static final class StateRound implements PendingRound {
        private final String nonce;
        private final String viewId;
        private final Address provider;
        private final int maxStateBytes;
        private final int maxStateEntries;
        private Hashtable envelope;
        private boolean invalid;

        StateRound(String nonce, String viewId, Address provider) {
            this(nonce, viewId, provider, Integer.MAX_VALUE, Integer.MAX_VALUE);
        }

        StateRound(String nonce, String viewId, Address provider, int maxStateBytes,
                   int maxStateEntries) {
            requireText(nonce, "nonce");
            requireText(viewId, "viewId");
            if (provider == null) {
                throw new IllegalArgumentException("provider is required");
            }
            this.nonce = nonce;
            this.viewId = viewId;
            this.provider = provider;
            this.maxStateBytes = maxStateBytes;
            this.maxStateEntries = maxStateEntries;
        }

        synchronized void accept(Address source, Object payload) {
            if (invalid || envelope != null) {
                invalid = true;
                return;
            }
            if (source == null || !provider.equals(source)
                    || !(payload instanceof Hashtable)) {
                invalid = true;
                return;
            }
            Hashtable candidate = (Hashtable) payload;
            if (!isCorrelated(candidate, SwarmSessionEnvelope.TYPE, nonce, viewId,
                    provider.toString())) {
                return;
            }
            try {
                envelope = SwarmSessionEnvelope.validate(payload, nonce, viewId, provider,
                        source, maxStateBytes, maxStateEntries);
            } catch (RuntimeException malformed) {
                invalid = true;
            }
        }

        synchronized Hashtable complete(String currentViewId) {
            if (invalid || envelope == null || !viewId.equals(currentViewId)) {
                return null;
            }
            return SwarmSessionEnvelope.validate(envelope, nonce, viewId, provider,
                    provider, maxStateBytes, maxStateEntries);
        }

        public synchronized boolean isTerminal() {
            return invalid || envelope != null;
        }

        public synchronized void invalidate() {
            invalid = true;
        }

        public String getViewId() {
            return viewId;
        }
    }

    private static boolean isCorrelated(Hashtable message, String type, String nonce,
                                        String viewId, String provider) {
        return PROTOCOL_VERSION.equals(message.get("protocol"))
                && type.equals(message.get("type"))
                && nonce.equals(message.get("nonce"))
                && viewId.equals(message.get("viewId"))
                && provider.equals(message.get("provider"));
    }
}
