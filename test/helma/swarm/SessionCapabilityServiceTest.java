package helma.swarm;

import java.util.Arrays;
import java.util.Hashtable;
import java.util.List;

import org.jgroups.Address;
import org.jgroups.util.UUID;

final class SessionCapabilityServiceTest {

    private SessionCapabilityServiceTest() {
    }

    static void register(AllTests.Suite suite) {
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

    private static Address address(long id) {
        return new UUID(0, id);
    }
}
