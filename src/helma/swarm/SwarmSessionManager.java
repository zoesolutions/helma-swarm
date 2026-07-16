/*
 * Helma License Notice
 *
 * The contents of this file are subject to the Helma License
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. A copy of the License is available at
 * http://adele.helma.org/download/helma/license.txt
 *
 * Copyright 1998-2003 Helma Software. All Rights Reserved.
 *
 * $RCSfile$
 * $Author$
 * $Revision$
 * $Date$
 */

package helma.swarm;

import org.jgroups.*;
import org.jgroups.blocks.PullPushAdapter;
import org.apache.commons.logging.Log;

import java.io.*;
import java.util.*;

import helma.framework.core.SessionManager;
import helma.framework.core.Application;
import helma.framework.core.Session;
import helma.framework.core.RequestEvaluator;
import helma.scripting.ScriptingEngineInterface;
import helma.objectmodel.db.NodeHandle;
import helma.objectmodel.NodeInterface;

public class SwarmSessionManager extends SessionManager
                                 implements MessageListener, Runnable {

    // SessionIdList operation constants
    static final int TOUCH = 0;
    static final int DISCARD = 1;

    PullPushAdapter adapter;
    Address address;
    Log log;
    volatile Thread runner;
    Set touched = Collections.synchronizedSet(new HashSet());
    Set discarded = Collections.synchronizedSet(new HashSet());
    boolean debug;

    private final Object bootstrapLock = new Object();
    private volatile boolean strictMode;
    private volatile boolean sessionStateInitialized;
    private volatile boolean bootstrapStarted;
    private volatile String sessionStateStatus = "NOT_STARTED";
    private volatile String sessionStateProvider = "";
    private volatile String knownSessionStateProviders = "";
    private volatile int lastReceivedStateSessionCount;
    private volatile String sessionStateLastError = "";
    private SwarmLifecycle lifecycle;
    private SessionCapabilityService controlService;
    private SwarmJoinPolicy strictPolicy;
    private BootstrapBuffer bootstrapBuffer;

    ////////////////////////////////////////////////////////
    // SessionManager functionality

    public void init(Application app) {
        super.init(app);
        String logName = new StringBuffer("helma.")
                                  .append(app.getName())
                                  .append(".swarm")
                                  .toString();
        log = app.getLogger(logName);
        debug = log.isDebugEnabled();
        try {
            adapter = ChannelUtils.getAdapter(app);
            Channel channel = (Channel) adapter.getTransport();
            // enable state exchange
            channel.setOpt(Channel.GET_STATE_EVENTS, Boolean.TRUE);
            channel.setOpt(Channel.AUTO_GETSTATE, Boolean.TRUE);
            address = channel.getLocalAddress();
            lifecycle = ChannelUtils.getExistingLifecycle(app);
            strictMode = lifecycle != null && lifecycle.getPolicy().isStrict();
            if (!strictMode) {
                adapter.setListener(this);
                if (!channel.getState(null, 5000)) {
                    log.debug("Couldn't get session state. First instance in swarm?");
                }
                sessionStateStatus = "LEGACY";
                startBroadcaster();
                return;
            }

            channel.setOpt(Channel.AUTO_GETSTATE, Boolean.FALSE);
            strictPolicy = lifecycle.getPolicy();
            bootstrapBuffer = new BootstrapBuffer(
                    strictPolicy.getBootstrapBufferMaxMessages(),
                    strictPolicy.getBootstrapBufferMaxBytes(),
                    strictPolicy.getBootstrapBufferMaxEntries());
            adapter.setListener(this);
            lifecycle.setCapability(SwarmLifecycle.Capability.SESSION_STARTING);
            controlService = lifecycle.getControlService();
            if (controlService == null) {
                throw new IllegalStateException("STRICT session control service is unavailable");
            }
            controlService.attachSessionManager(this);
            sessionStateStatus = "BUFFERING";
            if (!persistentSessionsEnabled()) {
                bootstrapStrictSessions(null);
            }
        } catch (Exception e) {
            sessionStateStatus = "ERROR";
            sessionStateLastError = SwarmLifecycle.safeError(e);
            log.error("HelmaSwarm: Error starting/joining channel", e);
            if (strictMode) {
                if (lifecycle != null) {
                    lifecycle.setCapability(SwarmLifecycle.Capability.MANAGER_PENDING);
                    lifecycle.configurationError(e);
                }
                waitForProcessShutdown();
            }
        }
    }

    public void loadSessionData(File file, ScriptingEngineInterface engine) {
        if (strictMode && sessionStateInitialized) {
            log.warn("SwarmSession: Ignoring late persistent-session reload after STRICT initialization");
            return;
        }
        super.loadSessionData(file, engine);
        if (strictMode) {
            bootstrapStrictSessions(engine);
        }
    }


    public Session createSession(String sessionId) {
        Session session = getSession(sessionId);
        if (session == null) {
            session = new SwarmSession(sessionId, app, this);
        }
        return session;
    }

    public void shutdown() {
        if (adapter != null) {
            adapter.setListener(null);
        }
        ChannelUtils.stopAdapter(app);

        if (runner != null) {
            Thread t = runner;
            runner = null;
            t.interrupt();
        }
        sessionStateInitialized = false;
        sessionStateStatus = "STOPPED";
    }

    private void startBroadcaster() {
        if (runner != null) {
            return;
        }
        runner = new Thread(this, "SwarmSessionMgr-" + app.getName());
        runner.setDaemon(true);
        runner.start();
    }


    public void discardSession(Session session) {
        super.discardSession(session);
        if (((SwarmSession) session).isDistributed()) {
            discarded.add(session.getSessionId());
        }
    }

    public void touchSession(SwarmSession session) {
        if (session.isDistributed()) {
            touched.add(session.getSessionId());
        }
    }

    ///////////////////////////////////////////////////////////////
    // Runnable

    public void run() {
        while(runner == Thread.currentThread()) {
            try {
                Thread.sleep(2000l);
            } catch (InterruptedException x) {
                log.info("SwarmSession: broadcast thread interrupted, exiting");
                return;
            }
            broadcastIds(TOUCH, touched);
            broadcastIds(DISCARD, discarded);
        }
    }

    ///////////////////////////////////////////////////////////////
    // JGroups/ MessageListener functionality

    public void receive(Message msg) {
        if (msg == null) {
            return;
        }
        if (address != null && address.equals(msg.getSrc())) {
            if (debug) {
                log.trace("Discarding own message: " + address);
            }
            return;
        }

        Object object;
        try {
            object = msg.getObject();
        } catch (RuntimeException malformed) {
            recordLivePayloadFailure(malformed);
            return;
        }
        if (strictMode) {
            receiveStrict(object);
            return;
        }
        if (debug) log.trace("Received object: " + object);
        if (object instanceof byte[]) {
            try {
                SwarmSession session = (SwarmSession) bytesToObject((byte[]) object);
                session.setApp(app);
                session.sessionMgr = this;
                Session local = getSession(session.getSessionId());
                if (local == null) {
                    registerSession(session);
                } else {
                    mergeSession(local, session);
                }
                if (debug) {
                    log.debug("Received session: " + session);
                }
            } catch (Exception x) {
                log.error("Error in session deserialization", x);
            }
        } else if (object instanceof SessionUpdate) {
            try {
                SessionUpdate update = (SessionUpdate) object;
                NodeInterface cacheNode = null;
                if (update.cacheNodeChanged) {
                    cacheNode = (NodeInterface) bytesToObject(update.cacheNode);
                    if (cacheNode == null) {
                        if (debug) {
                            log.debug("Discarded session update with empty cache node: " + update.sessionId);
                        }
                        return;
                    }
                }
                Session session = getSession(update.sessionId);
                if (session == null) {
                    if (!update.cacheNodeChanged) {
                        if (debug) {
                            log.debug("Discarded session update without local base: " + update.sessionId);
                        }
                        return;
                    }
                    session = createSession(update.sessionId);
                    mergeUpdate(session, update, cacheNode, true);
                    registerSession(session);
                } else {
                    mergeUpdate(session, update, cacheNode, false);
                }
                if (debug) {
                    log.debug("Received session update: " + session);
                }
            } catch (Exception x) {
                log.error("Error in session deserialization", x);
            }
        } else if (object instanceof SessionIdList) {
            SessionIdList idlist = (SessionIdList) object;
            Object[] ids = idlist.ids;
            if (idlist.operation == DISCARD) {
                // TODO: implement staged session dump
                for (int i = 0; i < ids.length; i++) {
                    sessions.remove(ids[i]);
                    if (debug) {
                        log.trace("Discarded session: " + ids[i]);
                    }
                }
            } else if (idlist.operation == TOUCH) {
                for (int i = 0; i < ids.length; i++) {
                    Object session = sessions.get(ids[i]);
                    if (session instanceof SwarmSession) {
                        ((SwarmSession) session).replicatedTouch();
                        if (debug) {
                            log.trace("Touched session: " + ids[i]);
                        }
                    }
                }
            }
        }
    }

    private void receiveStrict(Object payload) {
        if (!isLivePayload(payload)) {
            recordLivePayloadFailure(new IllegalArgumentException(
                    "unsupported live session payload"));
            return;
        }
        try {
            BootstrapBuffer.validatePayload(payload,
                    strictPolicy.getStateMaxBytes(),
                    strictPolicy.getStateMaxEntries());
        } catch (IllegalArgumentException oversized) {
            recordLivePayloadFailure(oversized);
            return;
        }
        synchronized (bootstrapLock) {
            if (!sessionStateInitialized) {
                try {
                    bootstrapBuffer.add(payload);
                } catch (RuntimeException overflow) {
                    sessionStateStatus = "ERROR";
                    sessionStateLastError = SwarmLifecycle.safeError(overflow);
                }
                return;
            }
            try {
                applyPayload(payload, sessions);
            } catch (Exception malformed) {
                recordLivePayloadFailure(malformed);
            }
        }
    }

    private static boolean isLivePayload(Object payload) {
        return payload instanceof byte[] || payload instanceof SessionUpdate
                || payload instanceof SessionIdList;
    }

    void recordLivePayloadFailure(Throwable failure) {
        synchronized (bootstrapLock) {
            sessionStateInitialized = false;
            sessionStateStatus = "ERROR";
            sessionStateLastError = SwarmLifecycle.safeError(failure);
            if (bootstrapBuffer != null) {
                bootstrapBuffer.invalidate();
            }
        }
        if (lifecycle != null) {
            lifecycle.setCapability(SwarmLifecycle.Capability.SESSION_STARTING);
        }
        if (log != null) {
            log.error("SwarmSession: Rejected malformed replicated session payload", failure);
        }
    }


    public byte[] getState() {
        if (strictMode) {
            return null;
        }
        Map map = getSessions();
        Hashtable state = new Hashtable();
        RequestEvaluator reval = app.getEvaluator();
        try {
            Iterator it = map.values().iterator();
            while (it.hasNext()) {
                Object obj = it.next();
                if (!(obj instanceof SwarmSession)) {
                    log.warn("SwarmSession: Skipping non-swarm session in getState(): " + obj);
                    continue;
                }
                SwarmSession session = (SwarmSession) obj;
                try {
                    state.put(session.getSessionId(), objectToBytes(session, reval));
                } catch (NotSerializableException x) {
                    log.warn("SwarmSession: Skipping non-serializable session in getState(): " + session, x);
                } catch (IOException x) {
                    log.warn("SwarmSession: Skipping session after serialization error in getState(): " + session, x);
                }
            }
            return objectToBytes(state, reval);
        } catch (IOException x) {
            log.error("Error in getState()", x);
            throw new RuntimeException("Error in getState(): "+x);
        } finally {
            if (debug) {
                log.debug("Returned session table: " + state.keySet());
            }
            app.releaseEvaluator(reval);
        }
    }

    public void setState(byte[] bytes) {
        if (strictMode) {
            return;
        }
        if (bytes != null) {
            try {
                Hashtable map = (Hashtable) bytesToObject(bytes);
                Iterator it = map.entrySet().iterator();
                while (it.hasNext()) {
                    Map.Entry entry = (Map.Entry) it.next();
                    try {
                        Object value = entry.getValue();
                        SwarmSession session = value instanceof byte[] ?
                                (SwarmSession) bytesToObject((byte[]) value) : (SwarmSession) value;
                        session.setApp(app);
                        session.sessionMgr = SwarmSessionManager.this;
                        Session local = getSession(session.getSessionId());
                        if (local == null) {
                            registerSession(session);
                        } else {
                            mergeSession(local, session);
                        }
                    } catch (Exception x) {
                        log.warn("SwarmSession: Skipping session from received state: " + entry.getKey(), x);
                    }
                }
                if (debug) {
                    log.debug("Received session map: " + map.keySet());
                }
            } catch (Exception x) {
                log.error ("Error in setState()", x);
            }
        }
    }

    private void mergeSession(Session local, SwarmSession remote) {
        synchronized (local) {
            if (remote.lastModified() >= local.lastModified()) {
                local.setMessage(remote.getMessage());
                local.setDebugBuffer(remote.getDebugBuffer());
                local.setUserHandle(remote.getUserHandle());
                local.setUID(remote.getUID());
                local.setLastModified(remote.lastModified());
            }
            NodeInterface remoteCache = remote.getCacheNode();
            NodeInterface localCache = local.getCacheNode();
            if (remoteCache != null && (localCache == null ||
                    remoteCache.lastModified() >= localCache.lastModified())) {
                local.setCacheNode(remoteCache);
            }
            if (local instanceof SwarmSession) {
                ((SwarmSession) local).setDistributed(remote.isDistributed());
            }
        }
    }

    private void mergeUpdate(Session session, SessionUpdate update, NodeInterface cacheNode,
                             boolean newSession) {
        synchronized (session) {
            if (newSession || update.lastModified >= session.lastModified()) {
                session.setMessage(update.message);
                session.setDebugBuffer(update.debugBuffer);
                session.setUserHandle(update.userHandle);
                session.setUID(update.uid);
                session.setLastModified(update.lastModified);
            }
            if (update.cacheNodeChanged && cacheNode != null) {
                NodeInterface localCache = session.getCacheNode();
                if (newSession || localCache == null ||
                        update.cacheNodeLastModified >= localCache.lastModified()) {
                    session.setCacheNode(cacheNode);
                }
            }
        }
    }

    Hashtable createStateEnvelope(String nonce, String viewId, String provider) {
        if (strictMode && !sessionStateInitialized) {
            throw new IllegalStateException("session state is not initialized");
        }
        Hashtable snapshot = (Hashtable) sessions.clone();
        validateStateEntryCount(snapshot.size(), strictPolicy.getStateMaxEntries());
        Hashtable encoded = new Hashtable();
        int total = snapshot.size();
        int exported = 0;
        int skipped = 0;
        int exportErrors = 0;
        int serializedBytes = 0;
        RequestEvaluator evaluator = app.getEvaluator();
        try {
            Iterator entries = snapshot.values().iterator();
            while (entries.hasNext()) {
                Object value = entries.next();
                if (!(value instanceof SwarmSession)) {
                    skipped++;
                    continue;
                }
                SwarmSession session = (SwarmSession) value;
                try {
                    byte[] serialized;
                    synchronized (session) {
                        serialized = objectToBytes(session,
                                evaluator.getScriptingEngine(),
                                strictPolicy.getStateMaxBytes() - serializedBytes);
                    }
                    encoded.put(session.getSessionId(), serialized);
                    serializedBytes += serialized.length;
                    exported++;
                } catch (SizeLimitExceededException exceeded) {
                    throw new IllegalStateException(
                            "session-state export exceeds configured byte limit", exceeded);
                } catch (Exception serializationFailure) {
                    exportErrors++;
                    if (debug) {
                        log.debug("SwarmSession: Session export failed", serializationFailure);
                    }
                }
            }
            byte[] state = objectToBytes(encoded, evaluator.getScriptingEngine(),
                    strictPolicy.getStateMaxBytes());
            return SwarmSessionEnvelope.create(nonce, viewId, provider, total, exported,
                    skipped, exportErrors, state, strictPolicy.getStateMaxBytes(),
                    strictPolicy.getStateMaxEntries());
        } catch (IOException serializationFailure) {
            throw new IllegalStateException("session-state map serialization failed",
                    serializationFailure);
        } finally {
            app.releaseEvaluator(evaluator);
        }
    }

    Hashtable importState(byte[] bytes) throws IOException, ClassNotFoundException {
        return importState(bytes, null);
    }

    Hashtable importState(byte[] bytes, ScriptingEngineInterface engine)
            throws IOException, ClassNotFoundException {
        return importState(bytes, engine, Integer.MAX_VALUE, Integer.MAX_VALUE);
    }

    private Hashtable importState(byte[] bytes, ScriptingEngineInterface engine,
                                  int maxStateBytes, int maxStateEntries)
            throws IOException, ClassNotFoundException {
        if (bytes == null || bytes.length > maxStateBytes) {
            throw new IOException("session-state payload exceeds configured byte limit");
        }
        Object decoded = decode(bytes, engine);
        if (!(decoded instanceof Hashtable)) {
            throw new IOException("session-state payload is not a Hashtable");
        }
        Hashtable encoded = (Hashtable) decoded;
        if (encoded.size() > maxStateEntries) {
            throw new IOException("session-state payload exceeds configured entry limit");
        }
        Hashtable temporary = new Hashtable();
        Iterator entries = encoded.entrySet().iterator();
        while (entries.hasNext()) {
            Map.Entry entry = (Map.Entry) entries.next();
            if (!(entry.getKey() instanceof String) || !(entry.getValue() instanceof byte[])) {
                throw new IOException("session-state entry has invalid key or value type");
            }
            Object value = decode((byte[]) entry.getValue(), engine);
            if (!(value instanceof SwarmSession)) {
                throw new IOException("session-state entry is not a SwarmSession");
            }
            SwarmSession session = (SwarmSession) value;
            if (!entry.getKey().equals(session.getSessionId())) {
                throw new IOException("session-state entry key does not match session");
            }
            if (app != null) {
                session.setApp(app);
            }
            session.sessionMgr = this;
            temporary.put(entry.getKey(), session);
        }
        return temporary;
    }

    void applyPayload(Object payload, Hashtable target)
            throws IOException, ClassNotFoundException {
        applyPayload(payload, target, null);
    }

    private void applyPayload(Object payload, Hashtable target,
                              ScriptingEngineInterface engine)
            throws IOException, ClassNotFoundException {
        if (payload instanceof byte[]) {
            Object decoded = decode((byte[]) payload, engine);
            if (!(decoded instanceof SwarmSession)) {
                throw new IOException("replicated payload is not a SwarmSession");
            }
            SwarmSession remote = (SwarmSession) decoded;
            if (remote.getSessionId() == null) {
                throw new IOException("replicated session id is missing");
            }
            if (app != null) {
                remote.setApp(app);
            }
            remote.sessionMgr = this;
            Session local = (Session) target.get(remote.getSessionId());
            if (local == null) {
                target.put(remote.getSessionId(), remote);
            } else {
                mergeSession(local, remote);
            }
            return;
        }
        if (payload instanceof SessionUpdate) {
            SessionUpdate update = (SessionUpdate) payload;
            if (update.sessionId == null) {
                throw new IOException("replicated update session id is missing");
            }
            NodeInterface cacheNode = null;
            if (update.cacheNodeChanged) {
                if (update.cacheNode == null) {
                    throw new IOException("replicated update cache node is missing");
                }
                Object decoded = decode(update.cacheNode, engine);
                if (!(decoded instanceof NodeInterface)) {
                    throw new IOException("replicated update cache node is invalid");
                }
                cacheNode = (NodeInterface) decoded;
            }
            Session session = (Session) target.get(update.sessionId);
            boolean created = session == null;
            if (created) {
                if (!update.cacheNodeChanged) {
                    return;
                }
                session = new SwarmSession(update.sessionId, app, this);
            }
            mergeUpdate(session, update, cacheNode, created);
            if (created) {
                target.put(update.sessionId, session);
            }
            return;
        }
        if (payload instanceof SessionIdList) {
            SessionIdList idList = (SessionIdList) payload;
            if (idList.ids == null || (idList.operation != DISCARD
                    && idList.operation != TOUCH)) {
                throw new IOException("replicated session id list is invalid");
            }
            for (int i = 0; i < idList.ids.length; i++) {
                if (!(idList.ids[i] instanceof String)) {
                    throw new IOException("replicated session id is invalid");
                }
            }
            for (int i = 0; i < idList.ids.length; i++) {
                Object id = idList.ids[i];
                if (idList.operation == DISCARD) {
                    target.remove(id);
                } else {
                    Object session = target.get(id);
                    if (session instanceof SwarmSession) {
                        ((SwarmSession) session).replicatedTouch();
                    }
                }
            }
            return;
        }
        throw new IOException("unsupported replicated session payload");
    }

    static Hashtable selectInitialState(boolean coldSeed, Hashtable disk,
                                        Hashtable provider) {
        Hashtable selected = coldSeed ? disk : provider;
        return selected == null ? new Hashtable() : (Hashtable) selected.clone();
    }

    private boolean persistentSessionsEnabled() {
        return "true".equalsIgnoreCase(app.getProperty("persistentSessions", "false"));
    }

    private void bootstrapStrictSessions(ScriptingEngineInterface loadEngine) {
        synchronized (bootstrapLock) {
            if (bootstrapStarted || sessionStateInitialized) {
                return;
            }
            bootstrapStarted = true;
        }

        ProcessShutdown shutdown = ProcessShutdown.current();
        while (!shutdown.isShuttingDown()) {
            synchronized (bootstrapLock) {
                if (bootstrapBuffer.isOverflowed()) {
                    bootstrapBuffer.resetForRetry();
                }
            }
            try {
                sessionStateStatus = "DISCOVERING";
                SessionCapabilityService.CapabilityView view = controlService.discover(
                        strictPolicy.getDiscoveryTimeoutMillis());
                if (view == null) {
                    throw new IOException("session capability round did not complete");
                }
                knownSessionStateProviders = providerList(view.getReadyProviders());

                Address provider = view.getProvider();
                Hashtable temporary;
                int receivedCount = 0;
                if (provider == null) {
                    if (address == null || !address.equals(view.getSeed())) {
                        throw new IOException("waiting for deterministic cold seed");
                    }
                    sessionStateStatus = "SEEDING";
                    sessionStateProvider = "";
                    temporary = copyCurrentSessions(loadEngine,
                            strictPolicy.getStateMaxBytes(),
                            strictPolicy.getStateMaxEntries());
                } else {
                    sessionStateStatus = "REQUESTING_STATE";
                    sessionStateProvider = provider.toString();
                    Hashtable envelope = controlService.requestState(provider,
                            strictPolicy.getStateTransferTimeoutMillis());
                    if (envelope == null) {
                        throw new IOException("authoritative session state was not received");
                    }
                    temporary = importState(SwarmSessionEnvelope.state(envelope), loadEngine,
                            strictPolicy.getStateMaxBytes(), strictPolicy.getStateMaxEntries());
                    receivedCount = ((Integer) envelope.get("exported")).intValue();
                    if (temporary.size() != receivedCount) {
                        throw new IOException("session-state payload count does not match envelope");
                    }
                }

                if (!controlService.isCurrentBootstrapViewSufficient(view)) {
                    throw new IOException("initial session-state commit rejected: "
                            + controlService.bootstrapViewRejectionReason(view));
                }
                sessionStateStatus = "REPLAYING";
                commitStrictState(temporary, receivedCount, loadEngine);

                return;
            } catch (InterruptedException interrupted) {
                Thread.interrupted();
                sessionStateLastError = SwarmLifecycle.safeError(interrupted);
                if (ProcessShutdown.current().isShuttingDown()) {
                    return;
                }
                sessionStateStatus = "RETRYING";
                sleepForStrictRetry();
                continue;
            } catch (Exception failure) {
                sessionStateLastError = SwarmLifecycle.safeError(failure);
                sessionStateStatus = bootstrapBuffer.isOverflowed() ? "ERROR" : "RETRYING";
                sleepForStrictRetry();
            }
        }
        sessionStateStatus = "STOPPED";
    }

    void commitStrictState(Hashtable temporary, int receivedCount,
                           ScriptingEngineInterface engine)
            throws IOException, ClassNotFoundException {
        synchronized (bootstrapLock) {
            if (bootstrapBuffer.isOverflowed()) {
                throw new IOException("session bootstrap buffer overflowed");
            }
            List buffered = bootstrapBuffer.snapshot();
            for (int i = 0; i < buffered.size(); i++) {
                applyPayload(buffered.get(i), temporary, engine);
            }
            sessions = temporary;
            bootstrapBuffer.clear();
            lastReceivedStateSessionCount = receivedCount;
            sessionStateInitialized = true;
            sessionStateStatus = "INITIALIZED";
            sessionStateLastError = "";
            publishSessionReady();
            if (lifecycle.getJoinStatus() == SwarmLifecycle.JoinStatus.READY) {
                startBroadcaster();
            }
        }
    }

    void publishSessionReady() {
        ChannelUtils.commitSessionReady(app, this);
    }

    private Hashtable copyCurrentSessions(ScriptingEngineInterface suppliedEngine,
                                          int maxStateBytes, int maxStateEntries)
            throws IOException, ClassNotFoundException {
        Hashtable snapshot = (Hashtable) sessions.clone();
        if (snapshot.size() > maxStateEntries) {
            throw new IOException("persistent session state exceeds configured entry limit");
        }
        Hashtable copy = new Hashtable();
        Hashtable encoded = new Hashtable();
        int serializedBytes = 0;
        RequestEvaluator evaluator = null;
        try {
            ScriptingEngineInterface engine = suppliedEngine;
            if (engine == null) {
                evaluator = app.getEvaluator();
                engine = evaluator.getScriptingEngine();
            }
            Iterator entries = snapshot.entrySet().iterator();
            while (entries.hasNext()) {
                Map.Entry entry = (Map.Entry) entries.next();
                if (!(entry.getKey() instanceof String)
                        || !(entry.getValue() instanceof SwarmSession)) {
                    throw new IOException("persistent session state is not swarm-compatible");
                }
                byte[] bytes;
                synchronized (entry.getValue()) {
                    bytes = objectToBytes(entry.getValue(), engine,
                            maxStateBytes - serializedBytes);
                }
                serializedBytes += bytes.length;
                encoded.put(entry.getKey(), bytes);
                Object decoded = deserialize(bytes, engine);
                if (!(decoded instanceof SwarmSession)) {
                    throw new IOException("persistent session copy is invalid");
                }
                SwarmSession session = (SwarmSession) decoded;
                if (!entry.getKey().equals(session.getSessionId())) {
                    throw new IOException("persistent session key does not match session");
                }
                if (app != null) {
                    session.setApp(app);
                }
                session.sessionMgr = this;
                copy.put(entry.getKey(), session);
            }
            objectToBytes(encoded, engine, maxStateBytes);
            return copy;
        } finally {
            if (evaluator != null) {
                app.releaseEvaluator(evaluator);
            }
        }
    }

    private void waitForProcessShutdown() {
        while (!ProcessShutdown.current().isShuttingDown()) {
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException interrupted) {
                Thread.interrupted();
                sessionStateLastError = SwarmLifecycle.safeError(interrupted);
            }
        }
    }

    private static Object deserialize(byte[] bytes, ScriptingEngineInterface engine)
            throws IOException, ClassNotFoundException {
        ByteArrayInputStream input = new ByteArrayInputStream(bytes);
        return engine.deserialize(input);
    }

    private Object decode(byte[] bytes, ScriptingEngineInterface engine)
            throws IOException, ClassNotFoundException {
        return engine == null ? bytesToObject(bytes) : deserialize(bytes, engine);
    }

    private void sleepForStrictRetry() {
        long delay = strictPolicy == null ? 500L
                : strictPolicy.getDiscoveryRetryDelayMillis();
        long deadline = System.currentTimeMillis() + Math.max(0L, delay);
        while (!ProcessShutdown.current().isShuttingDown()) {
            long remaining = deadline - System.currentTimeMillis();
            if (remaining <= 0L) {
                return;
            }
            try {
                Thread.sleep(remaining);
            } catch (InterruptedException interrupted) {
                Thread.interrupted();
                sessionStateLastError = SwarmLifecycle.safeError(interrupted);
            }
        }
    }

    private static String providerList(List providers) {
        StringBuffer value = new StringBuffer();
        for (int i = 0; i < providers.size(); i++) {
            if (i > 0) {
                value.append(',');
            }
            value.append(providers.get(i));
        }
        return value.toString();
    }

    public boolean isSessionStateInitialized() {
        return sessionStateInitialized;
    }

    public String getSessionStateStatus() {
        return sessionStateStatus;
    }

    public String getSessionStateProvider() {
        return sessionStateProvider;
    }

    public String getKnownSessionStateProviders() {
        return knownSessionStateProviders;
    }

    public int getLastReceivedStateSessionCount() {
        return lastReceivedStateSessionCount;
    }

    public String getSessionStateLastError() {
        return sessionStateLastError;
    }

    public boolean isControlProtocolComplete() {
        return controlService != null && controlService.isControlProtocolComplete();
    }

    void broadcastSession(SwarmSession session, RequestEvaluator reval, boolean transferCacheNode) {
        if (!app.isRunning()) {
            return;
        }
        RequestEvaluator borrowed = null;
        boolean newDistribution = !session.isDistributed();
        try {
            if (reval == null) {
                // A request thread already holds its own evaluator across commit(), so
                // borrowing a second one here doubles pool demand. Skip rather than block
                // (Application.getEvaluator() would sleep up to 12s on an exhausted pool).
                if (!canBorrowEvaluatorWithoutBlocking()) {
                    log.warn("SwarmSession: Skipping session broadcast, evaluator pool exhausted (free="
                            + app.countFreeEvaluators() + ", total=" + app.countEvaluators() + ")");
                    return;
                }
                reval = borrowed = app.getEvaluator();
            }
            log.debug("SwarmSession: Broadcasting changed session: " + session);
            if (session.isDistributed()) {
                SessionUpdate update = new SessionUpdate(session, reval,
                        transferCacheNode, liveMaximumBytes());
                if (strictMode) {
                    BootstrapBuffer.validatePayload(update,
                            liveMaximumBytes(), liveMaximumEntries());
                    validateSerializablePayload(update, liveMaximumBytes());
                }
                adapter.send(new Message(null, address, update));
            } else {
                session.setDistributed(true);
                byte[] bytes = strictMode
                        ? objectToBytes(session, reval.getScriptingEngine(),
                                liveMaximumBytes())
                        : objectToBytes(session, reval);
                adapter.send(new Message(null, address, (Serializable) bytes));
            }
        } catch (Exception x) {
            if (newDistribution) {
                session.setDistributed(false);
            }
            if (strictMode) {
                recordLivePayloadFailure(x);
            } else {
                log.error("SwarmSession: Error in session replication", x);
            }
        } finally {
            if (borrowed != null) {
                app.releaseEvaluator(borrowed);
            }
        }
    }

    private boolean canBorrowEvaluatorWithoutBlocking() {
        if (app.countFreeEvaluators() > 0) {
            return true;
        }
        int maxThreads = Integer.parseInt(app.getProperty("maxThreads", "50").trim());
        return app.countEvaluators() < maxThreads;
    }

    void broadcastIds(int operation, Set idSet) {
        try {
            SessionIdList idlist;
            if (strictMode) {
                idlist = drainIds(operation, idSet,
                        liveMaximumBytes(), liveMaximumEntries());
                if (idlist == null) {
                    return;
                }
            } else {
                Object[] ids;
                synchronized (idSet) {
                    if (idSet.isEmpty()) {
                        return;
                    }
                    ids = idSet.toArray();
                    idSet.clear();
                }
                idlist = new SessionIdList(operation, ids);
            }
            adapter.send(new Message(null, address, idlist));
        } catch (Exception x) {
            if (strictMode) {
                recordLivePayloadFailure(x);
            } else {
                log.error("Error broadcasting session list", x);
            }
        }
    }

    private int liveMaximumBytes() {
        return strictPolicy == null ? Integer.MAX_VALUE
                : strictPolicy.getStateMaxBytes();
    }

    private int liveMaximumEntries() {
        return strictPolicy == null ? Integer.MAX_VALUE
                : strictPolicy.getStateMaxEntries();
    }

    static SessionIdList drainIds(int operation, Set idSet, int maximumBytes,
                                  int maximumEntries) throws IOException {
        synchronized (idSet) {
            if (idSet.isEmpty()) {
                return null;
            }
            ArrayList selected = new ArrayList(Math.min(idSet.size(), maximumEntries));
            Iterator iterator = idSet.iterator();
            while (iterator.hasNext() && selected.size() < maximumEntries) {
                Object id = iterator.next();
                if (!(id instanceof String)) {
                    throw new IOException("session id set contains invalid id");
                }
                selected.add(id);
            }
            int lower = 1;
            int upper = selected.size();
            int accepted = 0;
            while (lower <= upper) {
                int candidateSize = lower + (upper - lower) / 2;
                SessionIdList candidate = new SessionIdList(operation,
                        selected.subList(0, candidateSize).toArray());
                try {
                    BootstrapBuffer.validatePayload(candidate,
                            maximumBytes, maximumEntries);
                    validateSerializablePayload(candidate, maximumBytes);
                    accepted = candidateSize;
                    lower = candidateSize + 1;
                } catch (SizeLimitExceededException oversized) {
                    upper = candidateSize - 1;
                } catch (IllegalArgumentException oversized) {
                    upper = candidateSize - 1;
                }
            }
            if (accepted == 0) {
                Object oversized = idSet.iterator().next();
                idSet.remove(oversized);
                throw new SizeLimitExceededException();
            }
            List chunk = selected.subList(0, accepted);
            SessionIdList result = new SessionIdList(operation, chunk.toArray());
            idSet.removeAll(chunk);
            return result;
        }
    }

    static byte[] objectToBytes(Object obj, RequestEvaluator reval)
            throws IOException {
        return objectToBytes(obj, reval.getScriptingEngine());
    }

    private static byte[] objectToBytes(Object obj, ScriptingEngineInterface engine)
            throws IOException {
        return objectToBytes(obj, engine, Integer.MAX_VALUE);
    }

    static byte[] objectToBytes(Object obj, ScriptingEngineInterface engine,
                                int maximumBytes) throws IOException {
        if (maximumBytes < 1) {
            throw new SizeLimitExceededException();
        }
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        OutputStream out = new BoundedOutputStream(bytes, maximumBytes);
        engine.serialize(obj, out);
        return bytes.toByteArray();
    }

    static void validateSerializablePayload(Serializable payload,
                                            int maximumBytes) throws IOException {
        ObjectOutputStream out = new ObjectOutputStream(
                new BoundedOutputStream(new NullOutputStream(), maximumBytes));
        try {
            out.writeObject(payload);
            out.flush();
        } finally {
            out.close();
        }
    }

    static void validateStateEntryCount(int entries, int maximumEntries) {
        if (entries > maximumEntries) {
            throw new IllegalStateException(
                    "session-state export exceeds configured entry limit");
        }
    }

    Object bytesToObject(byte[] bytes)
            throws IOException, ClassNotFoundException {
        RequestEvaluator reval = app.getEvaluator();
        try {
            ScriptingEngineInterface engine = reval.getScriptingEngine();
            ByteArrayInputStream in = new ByteArrayInputStream(bytes);
            return engine.deserialize(in);
        } finally {
            app.releaseEvaluator(reval);
        }
    }

    static final class BootstrapBuffer {
        private final int maximumMessages;
        private final int maximumBytes;
        private final int maximumEntries;
        private final ArrayList payloads = new ArrayList();
        private long retainedBytes;
        private long retainedEntries;
        private boolean overflowed;

        BootstrapBuffer(int maximum) {
            this(maximum, Integer.MAX_VALUE, Integer.MAX_VALUE);
        }

        BootstrapBuffer(int maximumMessages, int maximumBytes, int maximumEntries) {
            if (maximumMessages < 1 || maximumBytes < 1 || maximumEntries < 1) {
                throw new IllegalArgumentException("bootstrap buffer maximum must be positive");
            }
            this.maximumMessages = maximumMessages;
            this.maximumBytes = maximumBytes;
            this.maximumEntries = maximumEntries;
        }

        synchronized void add(Object payload) {
            if (overflowed) {
                throw new IllegalStateException("session bootstrap buffer already overflowed");
            }
            long payloadBytes = payloadBytes(payload);
            long payloadEntries = payloadEntries(payload);
            if (payloads.size() >= maximumMessages
                    || retainedBytes + payloadBytes > maximumBytes
                    || retainedEntries + payloadEntries > maximumEntries) {
                overflowed = true;
                throw new IllegalStateException("session bootstrap buffer overflow");
            }
            payloads.add(copyPayload(payload));
            retainedBytes += payloadBytes;
            retainedEntries += payloadEntries;
        }

        static void validatePayload(Object payload, int maximumBytes,
                                    int maximumEntries) {
            if (payloadBytes(payload) > maximumBytes
                    || payloadEntries(payload) > maximumEntries) {
                throw new IllegalArgumentException(
                        "live session payload exceeds configured limit");
            }
        }

        synchronized List snapshot() {
            return new ArrayList(payloads);
        }

        synchronized void clear() {
            payloads.clear();
            retainedBytes = 0L;
            retainedEntries = 0L;
        }

        synchronized void resetForRetry() {
            clear();
            overflowed = false;
        }

        synchronized boolean isOverflowed() {
            return overflowed;
        }

        synchronized void invalidate() {
            overflowed = true;
        }

        private static Object copyPayload(Object payload) {
            if (payload instanceof byte[]) {
                return ((byte[]) payload).clone();
            }
            if (payload instanceof SessionUpdate) {
                return new SessionUpdate((SessionUpdate) payload);
            }
            if (payload instanceof SessionIdList) {
                SessionIdList list = (SessionIdList) payload;
                return new SessionIdList(list.operation,
                        list.ids == null ? null : (Object[]) list.ids.clone());
            }
            throw new IllegalArgumentException("unsupported live session payload");
        }

        private static long payloadBytes(Object payload) {
            if (payload instanceof byte[]) {
                return ((byte[]) payload).length;
            }
            if (payload instanceof SessionUpdate) {
                SessionUpdate update = (SessionUpdate) payload;
                return 128L + stringBytes(update.sessionId) + stringBytes(update.message)
                        + stringBytes(update.uid)
                        + stringBytes(update.debugBuffer == null
                                ? null : update.debugBuffer.toString())
                        + (update.cacheNode == null ? 0L : update.cacheNode.length);
            }
            if (payload instanceof SessionIdList) {
                SessionIdList list = (SessionIdList) payload;
                if (list.ids == null) {
                    throw new IllegalArgumentException("session id list is missing ids");
                }
                long bytes = 32L + 8L * list.ids.length;
                for (int i = 0; i < list.ids.length; i++) {
                    if (!(list.ids[i] instanceof String)) {
                        throw new IllegalArgumentException("session id list contains invalid id");
                    }
                    bytes += stringBytes((String) list.ids[i]);
                }
                return bytes;
            }
            throw new IllegalArgumentException("unsupported live session payload");
        }

        private static long payloadEntries(Object payload) {
            if (payload instanceof SessionIdList) {
                SessionIdList list = (SessionIdList) payload;
                return list.ids == null ? 0L : list.ids.length;
            }
            if (payload instanceof byte[] || payload instanceof SessionUpdate) {
                return 1L;
            }
            throw new IllegalArgumentException("unsupported live session payload");
        }

        private static long stringBytes(String value) {
            return value == null ? 0L : 40L + 2L * value.length();
        }
    }

    private static final class BoundedOutputStream extends OutputStream {
        private final OutputStream delegate;
        private final long maximumBytes;
        private long written;

        BoundedOutputStream(OutputStream delegate, long maximumBytes) {
            this.delegate = delegate;
            this.maximumBytes = maximumBytes;
        }

        public void write(int value) throws IOException {
            ensureCapacity(1);
            delegate.write(value);
            written++;
        }

        public void write(byte[] bytes, int offset, int length) throws IOException {
            if (bytes == null) {
                throw new NullPointerException("bytes");
            }
            if (offset < 0 || length < 0 || offset > bytes.length - length) {
                throw new IndexOutOfBoundsException();
            }
            ensureCapacity(length);
            delegate.write(bytes, offset, length);
            written += length;
        }

        private void ensureCapacity(int additional) throws IOException {
            if (additional > maximumBytes - written) {
                throw new SizeLimitExceededException();
            }
        }
    }

    private static final class SizeLimitExceededException extends IOException {
        SizeLimitExceededException() {
            super("serialized state exceeds configured byte limit");
        }
    }

    private static final class NullOutputStream extends OutputStream {
        public void write(int value) {
        }

        public void write(byte[] bytes, int offset, int length) {
        }
    }

    static class SessionIdList implements Serializable {
        int operation;
        Object[] ids;

        SessionIdList(int operation, Object[] ids) {
            this.operation = operation;
            this.ids = ids;
        }
    }

    static class SessionUpdate implements Serializable {
        private static final long serialVersionUID = 3875724113689813472L;

        String sessionId;
        String message;
        StringBuffer debugBuffer;
        NodeHandle userHandle;
        String uid;
        long lastModified;
        long cacheNodeLastModified;
        boolean cacheNodeChanged;
        byte[] cacheNode = null;

        SessionUpdate(String sessionId, long lastModified) {
            this.sessionId = sessionId;
            this.lastModified = lastModified;
        }

        private SessionUpdate(SessionUpdate source) {
            sessionId = source.sessionId;
            message = source.message;
            debugBuffer = source.debugBuffer == null ? null
                    : new StringBuffer(source.debugBuffer.toString());
            userHandle = source.userHandle;
            uid = source.uid;
            lastModified = source.lastModified;
            cacheNodeLastModified = source.cacheNodeLastModified;
            cacheNodeChanged = source.cacheNodeChanged;
            cacheNode = source.cacheNode == null ? null : (byte[]) source.cacheNode.clone();
        }

        SessionUpdate(SwarmSession session, RequestEvaluator reval, boolean transferCacheNode)
                throws IOException {
            this(session, reval, transferCacheNode, Integer.MAX_VALUE);
        }

        SessionUpdate(SwarmSession session, RequestEvaluator reval,
                      boolean transferCacheNode, int maximumBytes)
                throws IOException {
            this.sessionId = session.getSessionId();
            this.message = session.getMessage();
            this.debugBuffer = session.getDebugBuffer();
            this.userHandle = session.getUserHandle();
            this.uid = session.getUID();
            this.lastModified = session.lastModified();
            this.cacheNodeLastModified = session.getCacheNode().lastModified();
            this.cacheNodeChanged = transferCacheNode;
            // only transfer cache node if it has changed
            if (transferCacheNode) {
                this.cacheNode = objectToBytes(session.getCacheNode(),
                        reval.getScriptingEngine(), maximumBytes);
            }
        }
    }
}
