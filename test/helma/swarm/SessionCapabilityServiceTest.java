package helma.swarm;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import helma.framework.core.Application;
import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.View;
import org.jgroups.blocks.PullPushAdapter;
import org.jgroups.util.UUID;

final class SessionCapabilityServiceTest {

    private SessionCapabilityServiceTest() {
    }

    static void register(AllTests.Suite suite) {
        suite.reproduction("capability discovery rejects a view below startup minimum",
                new AllTests.CheckedRunnable() {
                    public void run() throws Exception {
                        MutableChannel channel = new MutableChannel(1L, 1);
                        SessionCapabilityService service = service(channel, 2);

                        SessionCapabilityService.CapabilityView view =
                                service.discover(0L);

                        AllTests.assertTrue(view == null,
                                "singleton capability view bypassed startup minimum two");
                        AllTests.assertTrue(!service.isProtocolComplete(),
                                "rejected singleton remained protocol-complete");
                    }
                });

        suite.reproduction("bootstrap view sufficiency is explicit and current",
                new AllTests.CheckedRunnable() {
                    public void run() throws Exception {
                        MutableChannel channel = new MutableChannel(1L, 2);
                        SessionCapabilityService service = service(channel, 2);
                        SessionCapabilityService.CapabilityView complete =
                                completeView(channel.getView());
                        installCompleteView(service, complete);

                        AllTests.assertEquals(Boolean.TRUE,
                                Boolean.valueOf(
                                        service.isCurrentBootstrapViewSufficient(complete)),
                                "current complete view two was rejected");

                        channel.setView(2L, 1);
                        AllTests.assertEquals(Boolean.FALSE,
                                Boolean.valueOf(
                                        service.isCurrentBootstrapViewSufficient(complete)),
                                "stale singleton view remained sufficient for initial commit");

                        AllTests.assertEquals(
                                "capability view size 1 < required 2",
                                service.bootstrapViewRejectionReason(complete),
                                "singleton rejection was not operator-readable");

                        channel.setView(3L, 2);
                        AllTests.assertEquals(
                                "capability view changed during bootstrap",
                                service.bootstrapViewRejectionReason(complete),
                                "same-size view replacement exposed an ambiguous reason");
                    }
                });

        suite.baseline("complete capability round selects only ready providers",
                new AllTests.CheckedRunnable() {
                    public void run() {
                        Address local = address(1);
                        Address sync = address(2);
                        Address ready = address(3);
                        SessionCapabilityService.CapabilityRound round = round(local, sync, ready);
                        round.accept(sync, response("nonce", "view", sync, "NON_SESSION"));
                        round.accept(ready, response("nonce", "view", ready, "SESSION_READY"));
                        SessionCapabilityService.CapabilityView view = round.complete("view");
                        AllTests.assertTrue(view != null, "complete round was rejected");
                        AllTests.assertEquals(ready, view.getProvider(),
                                "NON_SESSION member was selected as provider");
                        AllTests.assertEquals(Integer.valueOf(1),
                                Integer.valueOf(view.getReadyProviders().size()),
                                "only SESSION_READY members are providers");
                    }
                });

        suite.reproduction("capability discovery never targets the local member",
                new AllTests.CheckedRunnable() {
                    public void run() {
                        Address local = address(14);
                        Address peerA = address(15);
                        Address peerB = address(16);
                        List<Address> remote = SessionCapabilityService.remoteMembers(
                                Arrays.asList(local, peerA, peerB), local);
                        AllTests.assertEquals(Arrays.asList(peerA, peerB), remote,
                                "local address remained in discovery destinations");
                    }
                });

        suite.reproduction("missing and legacy capability replies fail closed",
                new AllTests.CheckedRunnable() {
                    public void run() {
                        Address local = address(4);
                        Address peerA = address(5);
                        Address peerB = address(6);
                        SessionCapabilityService.CapabilityRound missing = round(local, peerA, peerB);
                        missing.accept(peerA, response("nonce", "view", peerA, "NON_SESSION"));
                        AllTests.assertTrue(missing.complete("view") == null,
                                "missing peer was inferred as NON_SESSION");

                        SessionCapabilityService.CapabilityRound legacy = round(local, peerA);
                        legacy.accept(peerA, new byte[] {1});
                        AllTests.assertTrue(legacy.complete("view") == null,
                                "legacy response completed a strict round");
                    }
                });

        suite.reproduction("capability round rejects stale nonce view and source",
                new AllTests.CheckedRunnable() {
                    public void run() {
                        Address local = address(7);
                        Address peer = address(8);
                        Address outsider = address(9);

                        SessionCapabilityService.CapabilityRound nonce = round(local, peer);
                        nonce.accept(peer, response("old", "view", peer, "SESSION_READY"));
                        AllTests.assertTrue(nonce.complete("view") == null,
                                "stale nonce completed round");

                        SessionCapabilityService.CapabilityRound view = round(local, peer);
                        view.accept(peer, response("nonce", "old", peer, "SESSION_READY"));
                        AllTests.assertTrue(view.complete("view") == null,
                                "stale view completed round");

                        SessionCapabilityService.CapabilityRound source = round(local, peer);
                        source.accept(outsider,
                                response("nonce", "view", outsider, "SESSION_READY"));
                        source.accept(peer, response("nonce", "view", peer, "SESSION_READY"));
                        AllTests.assertTrue(source.complete("view") == null,
                                "unknown source did not invalidate immutable round");
                    }
                });

        suite.reproduction("cold seed is deterministic by immutable view order",
                new AllTests.CheckedRunnable() {
                    public void run() {
                        Address local = address(10);
                        Address first = address(11);
                        Address second = address(12);
                        SessionCapabilityService.CapabilityRound round = round(local, first, second);
                        round.accept(second,
                                response("nonce", "view", second, "SESSION_STARTING"));
                        round.accept(first,
                                response("nonce", "view", first, "SESSION_STARTING"));
                        SessionCapabilityService.CapabilityView complete = round.complete("view");
                        AllTests.assertTrue(complete != null, "complete cold round was rejected");
                        AllTests.assertEquals(local, complete.getSeed(),
                                "response order changed deterministic view-order seed");
                        AllTests.assertTrue(complete.getProvider() == null,
                                "starting member was exposed as state provider");
                    }
                });

        suite.reproduction("delayed prior state envelope cannot complete new attempt",
                new AllTests.CheckedRunnable() {
                    public void run() {
                        Address provider = address(13);
                        SessionCapabilityService.StateRound oldRound =
                                new SessionCapabilityService.StateRound("old", "view", provider);
                        Hashtable oldEnvelope = SwarmSessionEnvelope.create("old", "view", provider,
                                0, 0, 0, 0, new byte[0]);
                        oldRound.accept(provider, oldEnvelope);
                        AllTests.assertTrue(oldRound.complete("view") != null,
                                "old round fixture did not complete");

                        SessionCapabilityService.StateRound current =
                                new SessionCapabilityService.StateRound("new", "view", provider);
                        current.accept(provider, oldEnvelope);
                        AllTests.assertTrue(current.complete("view") == null,
                                "delayed envelope completed a later attempt");

                        SessionCapabilityService.StateRound retried =
                                new SessionCapabilityService.StateRound("newer", "view", provider);
                        retried.accept(provider, SwarmSessionEnvelope.create("newer", "view", provider,
                                0, 0, 0, 0, new byte[0]));
                        AllTests.assertTrue(retried.complete("view") != null,
                                "fresh correlated retry did not recover");
                    }
                });

        suite.reproduction("stale replies do not poison the active retry round",
                new AllTests.CheckedRunnable() {
                    public void run() {
                        Address local = address(17);
                        Address peer = address(18);
                        SessionCapabilityService.CapabilityRound capabilities =
                                round(local, peer);
                        capabilities.accept(peer,
                                response("old", "view", peer, "SESSION_READY"));
                        capabilities.accept(peer,
                                response("nonce", "view", peer, "SESSION_READY"));
                        AllTests.assertTrue(capabilities.complete("view") != null,
                                "stale capability reply poisoned the active retry");

                        SessionCapabilityService.StateRound state =
                                new SessionCapabilityService.StateRound(
                                        "current", "view", peer);
                        state.accept(peer, SwarmSessionEnvelope.create(
                                "old", "view", peer, 0, 0, 0, 0, new byte[0]));
                        state.accept(peer, SwarmSessionEnvelope.create(
                                "current", "view", peer, 0, 0, 0, 0, new byte[0]));
                        AllTests.assertTrue(state.complete("view") != null,
                                "stale state reply poisoned the active retry");
                    }
                });
    }

    private static SessionCapabilityService.CapabilityRound round(Address local,
                                                                   Address... members) {
        List<Address> view = Arrays.asList(combine(local, members));
        return new SessionCapabilityService.CapabilityRound("nonce", "view", view, local,
                "SESSION_STARTING");
    }

    private static Address[] combine(Address local, Address[] members) {
        Address[] combined = new Address[members.length + 1];
        combined[0] = local;
        System.arraycopy(members, 0, combined, 1, members.length);
        return combined;
    }

    private static Hashtable response(String nonce, String view, Address source,
                                      String capability) {
        return SessionCapabilityService.capabilityResponse(nonce, view, source, capability);
    }

    private static SessionCapabilityService service(MutableChannel channel, int minimum)
            throws Exception {
        Properties properties = new Properties();
        properties.setProperty("swarm.join.strict", "true");
        properties.setProperty("swarm.join.minViewSize", String.valueOf(minimum));
        properties.setProperty("swarm.session.stateProviderMode", "strict");
        properties.setProperty("swarm.session.memberRole", "session");
        properties.setProperty("sessionManagerImpl", "helma.swarm.SwarmSessionManager");
        SwarmLifecycle lifecycle = new SwarmLifecycle(SwarmJoinPolicy.from(properties));
        PullPushAdapter adapter = new PullPushAdapter(channel, null, null, false);
        lifecycle.publishPreReady(adapter);
        SessionCapabilityService service = new SessionCapabilityService(
                new Application("capability-view-test"), adapter, lifecycle);
        return service;
    }

    private static SessionCapabilityService.CapabilityView completeView(View view) {
        List<Address> members = new java.util.ArrayList<Address>(view.getMembers());
        Map<Address, String> capabilities = new LinkedHashMap<Address, String>();
        for (Address member : members) {
            capabilities.put(member, "SESSION_STARTING");
        }
        return new SessionCapabilityService.CapabilityView(
                view.getViewId().toString(), members, capabilities);
    }

    private static void installCompleteView(SessionCapabilityService service,
                                            SessionCapabilityService.CapabilityView view)
            throws Exception {
        Field complete = SessionCapabilityService.class.getDeclaredField("lastCompleteView");
        complete.setAccessible(true);
        complete.set(service, view);
        Field protocol = SessionCapabilityService.class.getDeclaredField("protocolComplete");
        protocol.setAccessible(true);
        protocol.setBoolean(service, true);
    }

    private static Address address(long id) {
        return new UUID(0, id);
    }

    private static final class MutableChannel extends JChannel {
        private final Address local = address(1000L);
        private View view;

        MutableChannel(long viewSequence, int size) {
            super(false);
            setView(viewSequence, size);
        }

        void setView(long viewSequence, int size) {
            java.util.ArrayList<Address> members = new java.util.ArrayList<Address>();
            members.add(local);
            for (int i = 1; i < size; i++) {
                members.add(address(1000L + i));
            }
            view = new View(local, viewSequence, members);
        }

        public View getView() {
            return view;
        }

        public Address getLocalAddress() {
            return local;
        }
    }
}
