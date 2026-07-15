package helma.swarm;

import java.util.Hashtable;

import org.jgroups.Address;
import org.jgroups.util.UUID;

final class SwarmSessionEnvelopeTest {

    private SwarmSessionEnvelopeTest() {
    }

    static void register(AllTests.Suite suite) {
        suite.baseline("complete correlated state envelope is accepted",
                new AllTests.CheckedRunnable() {
                    public void run() {
                        Address provider = address(1);
                        Hashtable envelope = envelope(provider, "nonce-a", "view-a", 2, 2, 0, 0);
                        Hashtable validated = SwarmSessionEnvelope.validate(envelope,
                                "nonce-a", "view-a", provider, provider);
                        AllTests.assertEquals(Integer.valueOf(2), validated.get("total"),
                                "export total changed");
                        AllTests.assertEquals(Integer.valueOf(2), validated.get("exported"),
                                "export count changed");
                    }
                });

        suite.baseline("state payload is defensively copied",
                new AllTests.CheckedRunnable() {
                    public void run() {
                        Address provider = address(2);
                        byte[] state = new byte[] {1, 2, 3};
                        Hashtable envelope = SwarmSessionEnvelope.create("nonce", "view", provider,
                                1, 1, 0, 0, state);
                        state[0] = 9;
                        Hashtable validated = SwarmSessionEnvelope.validate(envelope,
                                "nonce", "view", provider, provider);
                        byte[] first = SwarmSessionEnvelope.state(validated);
                        AllTests.assertEquals(Byte.valueOf((byte) 1), Byte.valueOf(first[0]),
                                "create retained caller-owned state bytes");
                        first[0] = 8;
                        byte[] second = SwarmSessionEnvelope.state(validated);
                        AllTests.assertEquals(Byte.valueOf((byte) 1), Byte.valueOf(second[0]),
                                "state accessor exposed mutable envelope bytes");
                    }
                });

        suite.reproduction("stale nonce is rejected", rejection(new EnvelopeMutation() {
            public void mutate(Hashtable envelope, Address provider) {
                envelope.put("nonce", "old-nonce");
            }
        }, "stale nonce was accepted"));

        suite.reproduction("stale view is rejected", rejection(new EnvelopeMutation() {
            public void mutate(Hashtable envelope, Address provider) {
                envelope.put("viewId", "old-view");
            }
        }, "stale view was accepted"));

        suite.reproduction("wrong provider source is rejected",
                new AllTests.CheckedRunnable() {
                    public void run() throws Exception {
                        final Address provider = address(3);
                        final Hashtable envelope = envelope(provider, "nonce", "view", 1, 1, 0, 0);
                        AllTests.expectRejected(new AllTests.CheckedRunnable() {
                            public void run() {
                                SwarmSessionEnvelope.validate(envelope, "nonce", "view", provider,
                                        address(4));
                            }
                        }, "envelope from a different source was accepted");
                    }
                });

        suite.reproduction("incomplete export counters are rejected",
                new AllTests.CheckedRunnable() {
                    public void run() throws Exception {
                        assertRejected(envelope(address(5), "nonce", "view", 2, 1, 1, 0),
                                address(5), "skipped export was accepted");
                        assertRejected(envelope(address(6), "nonce", "view", 2, 1, 0, 1),
                                address(6), "export error was accepted");
                        assertRejected(envelope(address(7), "nonce", "view", 2, 1, 0, 0),
                                address(7), "short export was accepted");
                    }
                });

        suite.reproduction("malformed and legacy state payloads are rejected",
                new AllTests.CheckedRunnable() {
                    public void run() throws Exception {
                        final Address provider = address(8);
                        AllTests.expectRejected(new AllTests.CheckedRunnable() {
                            public void run() {
                                SwarmSessionEnvelope.validate(new byte[] {1}, "nonce", "view",
                                        provider, provider);
                            }
                        }, "legacy raw bytes were accepted");
                        final Hashtable malformed = envelope(provider, "nonce", "view", 1, 1, 0, 0);
                        malformed.put("state", "not-bytes");
                        assertRejected(malformed, provider, "non-byte state was accepted");
                    }
                });

        suite.reproduction("state envelope enforces configured byte and entry limits",
                new AllTests.CheckedRunnable() {
                    public void run() throws Exception {
                        final Address provider = address(10);
                        final Hashtable envelope = SwarmSessionEnvelope.create(
                                "nonce", "view", provider, 2, 2, 0, 0,
                                new byte[] {1, 2, 3, 4});
                        AllTests.expectRejected(new AllTests.CheckedRunnable() {
                            public void run() {
                                SwarmSessionEnvelope.validate(envelope, "nonce", "view",
                                        provider, provider, 3, 2);
                            }
                        }, "state envelope exceeded its configured byte limit");
                        AllTests.expectRejected(new AllTests.CheckedRunnable() {
                            public void run() {
                                SwarmSessionEnvelope.validate(envelope, "nonce", "view",
                                        provider, provider, 4, 1);
                            }
                        }, "state envelope exceeded its configured entry limit");
                    }
                });
    }

    private static AllTests.CheckedRunnable rejection(final EnvelopeMutation mutation,
                                                       final String message) {
        return new AllTests.CheckedRunnable() {
            public void run() throws Exception {
                final Address provider = address(9);
                final Hashtable envelope = envelope(provider, "nonce", "view", 1, 1, 0, 0);
                mutation.mutate(envelope, provider);
                assertRejected(envelope, provider, message);
            }
        };
    }

    private static void assertRejected(final Hashtable envelope, final Address provider,
                                       String message) throws Exception {
        AllTests.expectRejected(new AllTests.CheckedRunnable() {
            public void run() {
                SwarmSessionEnvelope.validate(envelope, "nonce", "view", provider, provider);
            }
        }, message);
    }

    private static Hashtable envelope(Address provider, String nonce, String view,
                                      int total, int exported, int skipped, int errors) {
        return SwarmSessionEnvelope.create(nonce, view, provider, total, exported, skipped,
                errors, new byte[] {1, 2, 3});
    }

    private static Address address(long id) {
        return new UUID(0, id);
    }

    private interface EnvelopeMutation {
        void mutate(Hashtable envelope, Address provider);
    }
}
