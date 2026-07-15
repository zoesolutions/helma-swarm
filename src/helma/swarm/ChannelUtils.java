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
import helma.framework.core.SessionManager;
import helma.framework.repository.RepositoryInterface;
import helma.framework.repository.ResourceInterface;
import helma.framework.repository.FileResource;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import java.util.WeakHashMap;
import java.util.Iterator;
import java.io.File;

interface StrictChannelBootstrap {
    PullPushAdapter bootstrap(Application app, SwarmLifecycle lifecycle)
            throws ChannelException;
}

public class ChannelUtils {

    // weak hashmap for pull-push-adapters
    static WeakHashMap adapters = new WeakHashMap();
    static WeakHashMap lifecycles = new WeakHashMap();

    // Ids for multiplexing PullPushAdapter.
    // SwarmSessionManager acts as main listener so it can use state exchange. 
    final static Integer CACHE = new Integer(1);
    final static Integer IDGEN = new Integer(2);
    final static Integer SESSION_CONTROL = new Integer(3);

    static PullPushAdapter getAdapter(Application app)
            throws ChannelException {
        return getAdapter(app, new StrictChannelBootstrap() {
            public PullPushAdapter bootstrap(Application target,
                                             SwarmLifecycle lifecycle)
                    throws ChannelException {
                return new SwarmChannelBootstrap(target, lifecycle).bootstrap();
            }
        });
    }

    static PullPushAdapter getAdapter(Application app,
                                      StrictChannelBootstrap strictBootstrap)
            throws ChannelException {
        if (strictBootstrap == null) {
            throw new NullPointerException("strictBootstrap");
        }
        PullPushAdapter adapter = getExistingAdapter(app);
        if (adapter != null) {
            return adapter;
        }

        synchronized (app) {
            adapter = getExistingAdapter(app);
            if (adapter != null) {
                return adapter;
            }

            boolean strict;
            try {
                strict = SwarmJoinPolicy.strictRequested(app.getProperties());
            } catch (IllegalArgumentException invalidStrictFlag) {
                strict = true;
            }

            if (!strict) {
                SwarmConfig config = new SwarmConfig(app);
                Channel channel = new JChannel(config.getJGroupsProps());
                String groupName = app.getProperty("swarm.name", app.getName());
                channel.connect(groupName + "_swarm");
                adapter = new PullPushAdapter(channel);
                adapter.start();
                SwarmLifecycle lifecycle = new SwarmLifecycle(
                        SwarmJoinPolicy.from(app.getProperties()));
                lifecycle.publishLegacyReady(adapter);
                synchronized (adapters) {
                    adapters.put(app, adapter);
                    lifecycles.put(app, lifecycle);
                }
                ProcessShutdown.current().register(lifecycle);
                return adapter;
            }

            SwarmLifecycle lifecycle = new SwarmLifecycle(
                    SwarmJoinPolicy.strictDefaultsForInvalidConfiguration());
            synchronized (adapters) {
                lifecycles.put(app, lifecycle);
            }
            ProcessShutdown.current().register(lifecycle);
            adapter = strictBootstrap.bootstrap(app, lifecycle);
            synchronized (adapters) {
                if (!lifecycle.publishPreReady(adapter)) {
                    adapter.stop();
                    throw new ChannelException(
                            "HelmaSwarm bootstrap stopped for JVM shutdown");
                }
                adapters.put(app, adapter);
            }
            return adapter;
        }
    }

    static void stopAdapter(Application app) {
        PullPushAdapter adapter;
        SwarmLifecycle lifecycle;
        synchronized (app) {
            synchronized (adapters) {
                adapter = (PullPushAdapter) adapters.remove(app);
                lifecycle = (SwarmLifecycle) lifecycles.remove(app);
            }
            if (lifecycle != null) {
                ProcessShutdown.current().unregister(lifecycle);
                lifecycle.stop();
                return;
            }
            if (adapter != null) {
                Channel channel = (Channel) adapter.getTransport();
                adapter.stop();
                if (channel.isConnected())
                    channel.disconnect();
                if (channel.isOpen())
                    channel.close();
            }
        }
    }

    static PullPushAdapter getExistingAdapter(Application app) {
        synchronized (adapters) {
            return (PullPushAdapter) adapters.get(app);
        }
    }

    static SwarmLifecycle getExistingLifecycle(Application app) {
        synchronized (adapters) {
            return (SwarmLifecycle) lifecycles.get(app);
        }
    }

    static SessionCapabilityService getExistingControlService(Application app) {
        SwarmLifecycle lifecycle = getExistingLifecycle(app);
        return lifecycle == null ? null : lifecycle.getControlService();
    }

    static void commitSessionReady(Application app, SessionManager manager) {
        SwarmLifecycle lifecycle = getExistingLifecycle(app);
        if (lifecycle == null || !lifecycle.getPolicy().isStrict()) {
            return;
        }
        if (lifecycle.getMemberRole() != SwarmJoinPolicy.MemberRole.SESSION
                || manager == null || manager != app.getSessionManager()
                || !(manager instanceof SwarmSessionManager)) {
            lifecycle.configurationError(new IllegalStateException(
                    "memberRole=session requires the active SwarmSessionManager"));
            awaitProcessShutdown(lifecycle);
            return;
        }
        lifecycle.setCapability(SwarmLifecycle.Capability.SESSION_READY);
        lifecycle.commitReady();
    }

    static void commitNonSessionReady(Application app, SessionManager manager) {
        SwarmLifecycle lifecycle = getExistingLifecycle(app);
        if (lifecycle == null || !lifecycle.getPolicy().isStrict()) {
            return;
        }
        if (lifecycle.getMemberRole() != SwarmJoinPolicy.MemberRole.NON_SESSION
                || manager == null || manager != app.getSessionManager()
                || !"helma.swarm.SwarmNonSessionManager".equals(
                        manager.getClass().getName())) {
            lifecycle.configurationError(new IllegalStateException(
                    "memberRole=non-session requires the active SwarmNonSessionManager"));
            awaitProcessShutdown(lifecycle);
            return;
        }
        lifecycle.setCapability(SwarmLifecycle.Capability.NON_SESSION);
        lifecycle.commitReady();
    }

    private static void awaitProcessShutdown(SwarmLifecycle lifecycle) {
        ProcessShutdown shutdown = ProcessShutdown.current();
        while (!shutdown.isShuttingDown()) {
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException interrupted) {
                Thread.interrupted();
                lifecycle.configurationError(interrupted);
            }
        }
        lifecycle.stop();
    }

    private static Channel getExistingChannel(Application app) {
        PullPushAdapter adapter = getExistingAdapter(app);
        return adapter == null ? null : (Channel) adapter.getTransport();
    }

    public static boolean isJoinStateAvailable(Application app) {
        SwarmLifecycle lifecycle = getExistingLifecycle(app);
        return lifecycle != null;
    }

    public static String getJoinStatus(Application app) {
        SwarmLifecycle lifecycle = getExistingLifecycle(app);
        return lifecycle == null ? SwarmLifecycle.JoinStatus.NOT_STARTED.name()
                : lifecycle.getJoinStatus().name();
    }

    public static int getJoinAttempts(Application app) {
        SwarmLifecycle lifecycle = getExistingLifecycle(app);
        return lifecycle == null ? 0 : lifecycle.getAttemptCount();
    }

    public static String getJoinLastError(Application app) {
        SwarmLifecycle lifecycle = getExistingLifecycle(app);
        return lifecycle == null ? "" : lifecycle.getLastError();
    }

    private static SwarmSessionManager getExistingSessionManager(Application app) {
        SessionManager manager = app == null ? null : app.getSessionManager();
        return manager instanceof SwarmSessionManager
                ? (SwarmSessionManager) manager : null;
    }

    public static boolean isSessionStateAvailable(Application app) {
        return getExistingSessionManager(app) != null;
    }

    public static boolean isSessionStateInitialized(Application app) {
        SwarmSessionManager manager = getExistingSessionManager(app);
        return manager != null && manager.isSessionStateInitialized();
    }

    public static String getSessionStateStatus(Application app) {
        SwarmSessionManager manager = getExistingSessionManager(app);
        return manager == null ? "" : manager.getSessionStateStatus();
    }

    public static String getSessionStateProvider(Application app) {
        SwarmSessionManager manager = getExistingSessionManager(app);
        return manager == null ? "" : manager.getSessionStateProvider();
    }

    public static String getKnownSessionStateProviders(Application app) {
        SwarmSessionManager manager = getExistingSessionManager(app);
        return manager == null ? "" : manager.getKnownSessionStateProviders();
    }

    public static int getLastReceivedStateSessionCount(Application app) {
        SwarmSessionManager manager = getExistingSessionManager(app);
        return manager == null ? 0 : manager.getLastReceivedStateSessionCount();
    }

    public static String getSessionStateLastError(Application app) {
        SwarmSessionManager manager = getExistingSessionManager(app);
        return manager == null ? "" : manager.getSessionStateLastError();
    }

    public static boolean isControlProtocolComplete(Application app) {
        SwarmSessionManager manager = getExistingSessionManager(app);
        return manager != null && manager.isControlProtocolComplete();
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
