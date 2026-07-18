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
import org.w3c.dom.*;
import helma.framework.core.Application;
import helma.framework.repository.RepositoryInterface;
import helma.framework.repository.ResourceInterface;
import helma.framework.repository.FileResource;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import java.util.WeakHashMap;
import java.util.Iterator;
import java.util.Properties;
import java.util.Random;
import java.io.File;

public class ChannelUtils {

    private static final Object REGISTRY_LOCK = new Object();

    // weak hashmap for pull-push-adapters
    static WeakHashMap adapters = new WeakHashMap();
    private static final WeakHashMap bootstrapStates = new WeakHashMap();
    private static final Random JITTER_RANDOM = new Random();
    private static final StartupShutdownHook SHUTDOWN = new StartupShutdownHook(
            new StartupShutdownHook.HookInstaller() {
                public void addShutdownHook(Thread hook) {
                    Runtime.getRuntime().addShutdownHook(hook);
                }
            });
    private static final StartupChannelBootstrap.AdapterFactory ADAPTERS =
            new StartupChannelBootstrap.AdapterFactory() {
                public PullPushAdapter create(Channel channel) {
                    return new PullPushAdapter(channel, null, null, false);
                }

                public void start(PullPushAdapter adapter) {
                    adapter.start();
                }

                public void stop(PullPushAdapter adapter) {
                    adapter.stop();
                }
            };
    private static final StartupChannelBootstrap BOOTSTRAP =
            new StartupChannelBootstrap(new StartupChannelBootstrap.Dependencies(
                    new StartupChannelBootstrap.ChannelFactory() {
                        public Channel create(Application app) throws Exception {
                            return new JChannel(new SwarmConfig(app).getJGroupsProps());
                        }
                    }, ADAPTERS, new StartupChannelBootstrap.Scheduler() {
                        public long nowMillis() {
                            return System.currentTimeMillis();
                        }

                        public void sleep(long millis) throws InterruptedException {
                            Thread.sleep(millis);
                        }
                    }, new StartupChannelBootstrap.Jitter() {
                        public long delay(long capMillis) {
                            synchronized (JITTER_RANDOM) {
                                return StartupChannelBootstrap.equalJitterDelay(
                                        capMillis, JITTER_RANDOM.nextDouble());
                            }
                        }
                    }));

    // Ids for multiplexing PullPushAdapter.
    // SwarmSessionManager acts as main listener so it can use state exchange. 
    final static Integer CACHE = new Integer(1);
    final static Integer IDGEN = new Integer(2);

    static PullPushAdapter getAdapter(Application app)
            throws ChannelException {
        BootstrapState state;
        synchronized (REGISTRY_LOCK) {
            PullPushAdapter existing = (PullPushAdapter) adapters.get(app);
            if (existing != null) {
                return existing;
            }
            state = getOrCreateBootstrapStateLocked(app);
        }

        SHUTDOWN.register(state);
        final StartupJoinPolicy policy;
        try {
            Properties properties = app.getProperties();
            policy = StartupJoinPolicy.parse(properties);
        } catch (IllegalArgumentException invalid) {
            if (state.recordConfigurationFailure()) {
                app.logError("HelmaSwarm: invalid startup channel configuration");
            }
            try {
                return failStop(state);
            } finally {
                removeAbortedState(app, state);
                SHUTDOWN.deregister(state);
            }
        }

        try {
            PullPushAdapter adapter = policy.isEnabled()
                    ? BOOTSTRAP.bootstrap(app, state, policy)
                    : legacyBootstrap(app, state);
            if (!publishAdapter(app, state, adapter)) {
                throw new ChannelException("startup channel bootstrap cancelled");
            }
            SHUTDOWN.deregister(state);
            return adapter;
        } catch (ChannelException failure) {
            removeAbortedState(app, state);
            SHUTDOWN.deregister(state);
            throw failure;
        } catch (Exception failure) {
            removeAbortedState(app, state);
            SHUTDOWN.deregister(state);
            throw new ChannelException("startup channel bootstrap failed", failure);
        }
    }

    static void stopAdapter(Application app) {
        PullPushAdapter adapter;
        BootstrapState state;
        synchronized (REGISTRY_LOCK) {
            state = (BootstrapState) bootstrapStates.get(app);
            adapter = state == null
                    ? (PullPushAdapter) adapters.remove(app) : null;
        }
        if (state != null) {
            state.cancel();
        } else if (adapter != null) {
            Channel channel = (Channel) adapter.getTransport();
            if (channel.isConnected())
                channel.disconnect();
            if (channel.isOpen())
                channel.close();
            adapter.stop();
        }
    }

    static void registerSessionStartup(SessionStateStartupToken token) {
        SHUTDOWN.register(token);
    }

    static void deregisterSessionStartup(SessionStateStartupToken token) {
        SHUTDOWN.deregister(token);
    }

    private static PullPushAdapter legacyBootstrap(Application app,
                                                   BootstrapState state)
            throws ChannelException {
        if (!state.claimLeader()) {
            try {
                return state.awaitAdapter();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new ChannelException("legacy channel bootstrap interrupted",
                        interrupted);
            }
        }

        StartupCandidate candidate = null;
        try {
            Channel channel = new JChannel(new SwarmConfig(app).getJGroupsProps());
            candidate = new StartupCandidate(channel, ADAPTERS);
            if (!state.registerCandidate(candidate)) {
                state.cleanupUnregistered(candidate);
                throw new ChannelException("legacy channel bootstrap cancelled");
            }
            String groupName = app.getProperty("swarm.name", app.getName());
            channel.connect(groupName + "_swarm");
            PullPushAdapter adapter = new PullPushAdapter(channel);
            if (!candidate.attachAdapter(adapter)) {
                adapter.stop();
                throw new ChannelException("legacy channel bootstrap cancelled");
            }
            if (!candidate.startAdapter()) {
                throw new ChannelException("legacy channel bootstrap cancelled");
            }
            if (!state.publish(candidate)) {
                throw new ChannelException("legacy channel bootstrap cancelled");
            }
            return adapter;
        } catch (ChannelException failure) {
            cleanup(state, candidate);
            state.cancel();
            throw failure;
        } catch (Exception failure) {
            cleanup(state, candidate);
            state.cancel();
            throw new ChannelException("legacy channel bootstrap failed", failure);
        } finally {
            state.leaderFinished();
        }
    }

    private static PullPushAdapter failStop(BootstrapState state)
            throws ChannelException {
        boolean interrupted;
        if (state.claimLeader()) {
            try {
                interrupted = awaitFailStopCancellation(state);
            } finally {
                state.leaderFinished();
            }
        } else {
            interrupted = awaitFailStopCancellation(state);
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
        throw new ChannelException("invalid startup channel configuration");
    }

    private static boolean awaitFailStopCancellation(BootstrapState state) {
        boolean interrupted = false;
        while (!state.isCancelled()) {
            try {
                state.awaitCancellation();
            } catch (InterruptedException unexpected) {
                interrupted = true;
            }
        }
        return interrupted;
    }

    private static void cleanup(BootstrapState state,
                                StartupCandidate candidate) {
        StartupCandidate owned = state.takeCandidate(candidate);
        if (owned != null) {
            state.recordCleanupResult(owned.cleanup());
        }
    }

    private static void removeAbortedState(Application app,
                                           BootstrapState state) {
        if (!state.isCancellationComplete()) {
            return;
        }
        synchronized (REGISTRY_LOCK) {
            if (bootstrapStates.get(app) == state) {
                bootstrapStates.remove(app);
            }
        }
    }

    static BootstrapState createBootstrapState(final Application app) {
        BootstrapState state = new BootstrapState(
                new BootstrapState.CancellationListener() {
                    public void cancelled(BootstrapState cancelled) {
                        removeCancelledAdapter(app, cancelled);
                    }

                    public void finished(BootstrapState cancelled) {
                        removeCancelledState(app, cancelled);
                    }
                });
        synchronized (REGISTRY_LOCK) {
            bootstrapStates.put(app, state);
        }
        return state;
    }

    static BootstrapState getOrCreateBootstrapState(Application app) {
        synchronized (REGISTRY_LOCK) {
            return getOrCreateBootstrapStateLocked(app);
        }
    }

    private static BootstrapState getOrCreateBootstrapStateLocked(Application app) {
        BootstrapState state = (BootstrapState) bootstrapStates.get(app);
        return state == null ? createBootstrapState(app) : state;
    }

    static boolean publishAdapter(Application app, BootstrapState state,
                                  PullPushAdapter ignored) {
        final Application application = app;
        final BootstrapState expected = state;
        return state.publishToRegistry(new BootstrapState.PublicationAction() {
            public boolean publish(PullPushAdapter adapter) {
                synchronized (REGISTRY_LOCK) {
                    if (bootstrapStates.get(application) != expected) {
                        return false;
                    }
                    adapters.put(application, adapter);
                    return true;
                }
            }
        });
    }

    private static void removeCancelledState(Application app,
                                             BootstrapState state) {
        synchronized (REGISTRY_LOCK) {
            if (bootstrapStates.get(app) == state) {
                bootstrapStates.remove(app);
                adapters.remove(app);
            }
        }
    }

    private static void removeCancelledAdapter(Application app,
                                               BootstrapState state) {
        synchronized (REGISTRY_LOCK) {
            if (bootstrapStates.get(app) == state) {
                adapters.remove(app);
            }
        }
    }

    private static Channel getExistingChannel(Application app) {
        PullPushAdapter adapter;
        synchronized (REGISTRY_LOCK) {
            adapter = (PullPushAdapter) adapters.get(app);
        }
        return adapter == null ? null : (Channel) adapter.getTransport();
    }

    public static boolean isConnected(Application app) {
        try {
            Channel channel = getExistingChannel(app);
            return channel != null && channel.isConnected();
        } catch (Exception x) {
            app.logError("ChannelUtils.isConnected()", x);
        }
        return false;
    }

    public static int getViewSize(Application app) {
        try {
            Channel channel = getExistingChannel(app);
            View view = channel == null ? null : channel.getView();
            return view == null ? 0 : view.getMembers().size();
        } catch (Exception x) {
            app.logError("ChannelUtils.getViewSize()", x);
        }
        return 0;
    }

    public static String getView(Application app) {
        try {
            Channel channel = getExistingChannel(app);
            View view = channel == null ? null : channel.getView();
            return view == null ? "" : view.toString();
        } catch (Exception x) {
            app.logError("ChannelUtils.getView()", x);
        }
        return "";
    }

    public static boolean isMaster(Application app) {
        try {
            Channel channel = getExistingChannel(app);
            if (channel != null) {
                View view = channel.getView();
                Address address = channel.getLocalAddress();
                if (view != null && address != null && !view.getMembers().isEmpty()) {
                    return address.equals(view.getMembers().firstElement());
                }
            }
        } catch (Exception x) {
            app.logError("ChannelUtils.isMaster()", x);
        }
        return false;
    }
}

class SwarmConfig {

    String jGroupsProps =
            "UDP(mcast_addr=224.0.0.132;mcast_port=22024;ip_ttl=32;" +
                "bind_port=48848;port_range=1000;" +
                "mcast_send_buf_size=150000;mcast_recv_buf_size=80000):" +
            "PING(timeout=2000;num_initial_members=3):" +
            "MERGE2(min_interval=5000;max_interval=10000):" +
            "FD_SOCK:" +
            "VERIFY_SUSPECT(timeout=1500):" +
            "pbcast.NAKACK(gc_lag=50;retransmit_timeout=300,600,1200,2400,4800):" +
            "UNICAST(timeout=5000):" +
            "pbcast.STABLE(desired_avg_gossip=20000):" +
            "FRAG(frag_size=8096;down_thread=false;up_thread=false):" +
            "pbcast.GMS(join_timeout=5000;join_retry_timeout=2000;shun=false;print_local_addr=true):" +
            "pbcast.STATE_TRANSFER";

    public SwarmConfig (Application app) {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();

        ResourceInterface res = null;

        String conf = app.getProperty("swarm.conf");

        if (conf != null) {
            res = new FileResource(new File(conf));
        } else {
            Iterator reps = app.getRepositories().iterator();
            while (reps.hasNext()) {
                RepositoryInterface rep = (RepositoryInterface) reps.next();
                res = rep.getResource("swarm.conf");
                if (res != null)
                    break;
            }
        }

        if (res == null || !res.exists()) {
            app.logEvent("Resource \"" + conf + "\" not found, using defaults");
            return;
        }

        app.logEvent("HelmaSwarm: Reading config from " + res);

        try {
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document document = builder.parse(res.getInputStream());
            Element root = document.getDocumentElement();
            NodeList nodes = root.getElementsByTagName("jgroups-stack");

            if (nodes.getLength() == 0) {
                app.logEvent("No JGroups stack found in swarm.conf, using defaults");
            } else {
                NodeList jgroups = null;

                String stackName = app.getProperty("swarm.jgroups.stack", "udp");
                for (int i = 0; i < nodes.getLength(); i++) {
                    Element elem = (Element) nodes.item(i);
                    if (stackName.equalsIgnoreCase(elem.getAttribute("name"))) {
                        jgroups = elem.getChildNodes();
                        break;
                    }
                }
                if (jgroups == null) {
                    app.logEvent("JGroups stack \"" + stackName +
                            "\" not found in swarm.conf, using first element");
                    jgroups = nodes.item(0).getChildNodes();
                }

                StringBuffer buffer = new StringBuffer();
                for (int i = 0; i < jgroups.getLength(); i++) {
                    Node node = jgroups.item(i);
                    if (! (node instanceof Text)) {
                        continue;
                    }
                    String str = ((Text) node).getData();
                    for (int j = 0; j < str.length(); j++) {
                        char c = str.charAt(j);
                        if (!Character.isWhitespace(c)) {
                            buffer.append(c);
                        }
                    }
                }
                if (buffer.length() > 0) {
                    jGroupsProps = buffer.toString();
                }
            }

        } catch (Exception e) {
            app.logError("HelmaSwarm: Error reading config from " + res, e);
        }
    }

    public String getJGroupsProps() {
        return jGroupsProps;
    }

}
