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
    volatile boolean initialStateAttemptActive;
    volatile boolean initialStateApplied;
    volatile boolean initialStateCompleted;
    private static final Random SESSION_RETRY_RANDOM = new Random();
    private final Object initialStateMonitor = new Object();

    ////////////////////////////////////////////////////////
    // SessionManager functionality

    public void init(Application app) {
        this.app = app;
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
            // register us as main message listeners so we can exchange state
            adapter.setListener(this);
            StartupJoinPolicy policy = StartupJoinPolicy.parse(app.getProperties());
            if (policy.isEnabled() && policy.getMinViewSize() > 1) {
                synchronizeInitialState(channel);
            } else if (!channel.getState(null, 5000)) {
                log.debug("Couldn't get session state. First instance in swarm?");
            }
            // start broadcaster thread
            runner = new Thread(this, "SwarmSessionMgr-" + app.getName());
            runner.setDaemon(true);
            runner.start();
        } catch (Exception e) {
            log.error("HelmaSwarm: Error starting/joining channel", e);
            e.printStackTrace();
        }
    }

    private void synchronizeInitialState(final Channel channel)
            throws ChannelException {
        View initialView = channel.getView();
        final boolean initialCoordinator = isInitialCoordinator(address, initialView);
        final SessionStateStartupToken token = new SessionStateStartupToken(
                new Runnable() {
                    public void run() {
                        ChannelUtils.stopAdapter(app);
                    }
                });
        ChannelUtils.registerSessionStartup(token);
        try {
            InitialSessionStateSynchronizer synchronizer =
                    new InitialSessionStateSynchronizer(
                            new InitialSessionStateSynchronizer.Transfer() {
                                public boolean request() throws Exception {
                                    return channel.getState(null, 5000);
                                }
                            }, new InitialSessionStateSynchronizer.ApplyAck() {
                                public void begin() {
                                    synchronized (initialStateMonitor) {
                                        initialStateApplied = false;
                                        initialStateCompleted = false;
                                        initialStateAttemptActive = true;
                                    }
                                }

                                public boolean isApplied() {
                                    return initialStateApplied;
                                }

                                public boolean awaitApplied(long timeoutMillis)
                                        throws InterruptedException {
                                    long deadline = timeoutMillis <= 0L ? 0L
                                            : System.currentTimeMillis() + timeoutMillis;
                                    synchronized (initialStateMonitor) {
                                        while (!initialStateCompleted) {
                                            if (deadline == 0L) {
                                                initialStateMonitor.wait();
                                            } else {
                                                long remaining = deadline
                                                        - System.currentTimeMillis();
                                                if (remaining <= 0L) {
                                                    return false;
                                                }
                                                initialStateMonitor.wait(remaining);
                                            }
                                        }
                                        return initialStateApplied;
                                    }
                                }

                                public void end() {
                                    initialStateAttemptActive = false;
                                }
                            }, new InitialSessionStateSynchronizer.SeedPolicy() {
                                public boolean maySeed() {
                                    return initialCoordinator && getSessions().isEmpty();
                                }
                            }, new InitialSessionStateSynchronizer.Delay() {
                                public long next(long capMillis) {
                                    long lower = capMillis / 2L;
                                    long width = capMillis - lower;
                                    synchronized (SESSION_RETRY_RANDOM) {
                                        return lower + (long) (SESSION_RETRY_RANDOM.nextDouble()
                                                * (width + 1L));
                                    }
                                }
                            }, token);
            synchronizer.synchronize();
        } finally {
            initialStateAttemptActive = false;
            ChannelUtils.deregisterSessionStartup(token);
        }
    }

    static boolean isInitialCoordinator(Address localAddress, View initialView) {
        Vector members = initialView == null ? null : initialView.getMembers();
        return localAddress != null && members != null && !members.isEmpty()
                && localAddress.equals(members.firstElement());
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
        if (address.equals(msg.getSrc())) {
            if (debug) {
                log.trace("Discarding own message: " + address);
            }
            return;
        }

        Object object = msg.getObject();
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


    public byte[] getState() {
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
        boolean startupAttempt = initialStateAttemptActive;
        boolean complete = startupAttempt
                ? applyInitialSessionState(bytes)
                : applyLegacySessionState(bytes);
        if (startupAttempt) {
            synchronized (initialStateMonitor) {
                initialStateApplied = complete;
                initialStateCompleted = true;
                initialStateMonitor.notifyAll();
            }
        }
    }

    private boolean applyInitialSessionState(byte[] bytes) {
        if (bytes == null) {
            return false;
        }
        try {
            Hashtable map = (Hashtable) bytesToObject(bytes);
            List decodedSessions = new ArrayList(map.size());
            Iterator it = map.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry entry = (Map.Entry) it.next();
                try {
                    decodedSessions.add(decodeSession(entry.getValue()));
                } catch (Exception x) {
                    log.warn("SwarmSession: Skipping session from received state: "
                            + entry.getKey(), x);
                    return false;
                }
            }
            Iterator decoded = decodedSessions.iterator();
            while (decoded.hasNext()) {
                applySession((SwarmSession) decoded.next());
            }
            if (debug) {
                log.debug("Received session map: " + map.keySet());
            }
            return true;
        } catch (Exception x) {
            log.error("Error in setState()", x);
            return false;
        }
    }

    private boolean applyLegacySessionState(byte[] bytes) {
        if (bytes == null) {
            return false;
        }
        try {
            Hashtable map = (Hashtable) bytesToObject(bytes);
            Iterator it = map.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry entry = (Map.Entry) it.next();
                try {
                    applySession(decodeSession(entry.getValue()));
                } catch (Exception x) {
                    log.warn("SwarmSession: Skipping session from received state: "
                            + entry.getKey(), x);
                }
            }
            if (debug) {
                log.debug("Received session map: " + map.keySet());
            }
            return true;
        } catch (Exception x) {
            log.error("Error in setState()", x);
            return false;
        }
    }

    private SwarmSession decodeSession(Object value) throws Exception {
        SwarmSession session = value instanceof byte[]
                ? (SwarmSession) bytesToObject((byte[]) value)
                : (SwarmSession) value;
        if (session == null || session.getSessionId() == null) {
            throw new IOException("invalid session state entry");
        }
        session.setApp(app);
        session.sessionMgr = SwarmSessionManager.this;
        return session;
    }

    private void applySession(SwarmSession session) {
        Session local = getSession(session.getSessionId());
        if (local == null) {
            registerSession(session);
        } else {
            mergeSession(local, session);
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

    void broadcastSession(SwarmSession session, RequestEvaluator reval, boolean transferCacheNode) {
        if (!app.isRunning()) {
            return;
        }
        RequestEvaluator borrowed = null;
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
                SessionUpdate update = new SessionUpdate(session, reval, transferCacheNode);
                adapter.send(new Message(null, address, update));
            } else {
                session.setDistributed(true);
                byte[] bytes = objectToBytes(session, reval);
                adapter.send(new Message(null, address, (Serializable) bytes));
            }
        } catch (Exception x) {
            log.error("SwarmSession: Error in session replication", x);
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
            Object[] ids;
            synchronized (idSet) {
                if (idSet.isEmpty()) {
                    return;
                }
                ids = idSet.toArray();
                idSet.clear();
            }
            Serializable idlist = new SessionIdList(operation, ids);
            adapter.send(new Message(null, address, idlist));
        } catch (Exception x) {
            log.error("Error broadcasting session list", x);
        }
    }

    static byte[] objectToBytes(Object obj, RequestEvaluator reval)
            throws IOException {
        ScriptingEngineInterface engine = reval.getScriptingEngine();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        engine.serialize(obj, out);
        return out.toByteArray();
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

        SessionUpdate(SwarmSession session, RequestEvaluator reval, boolean transferCacheNode)
                throws IOException{
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
                this.cacheNode = objectToBytes(session.getCacheNode(), reval);
            }
        }
    }
}
