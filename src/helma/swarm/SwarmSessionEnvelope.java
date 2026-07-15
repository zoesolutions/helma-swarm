/*
 * Helma License Notice
 *
 * The contents of this file are subject to the Helma License
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License.
 */
package helma.swarm;

import java.util.Hashtable;

import org.jgroups.Address;

final class SwarmSessionEnvelope {

    static final Integer PROTOCOL_VERSION = Integer.valueOf(1);
    static final String TYPE = "STATE_ENVELOPE";

    private SwarmSessionEnvelope() {
    }

    static Hashtable create(String nonce, String viewId, Address provider, int total,
                            int exported, int skipped, int exportErrors, byte[] state) {
        if (provider == null) {
            throw new IllegalArgumentException("provider is required");
        }
        return create(nonce, viewId, provider.toString(), total, exported, skipped,
                exportErrors, state);
    }

    static Hashtable create(String nonce, String viewId, Address provider, int total,
                            int exported, int skipped, int exportErrors, byte[] state,
                            int maxStateBytes, int maxStateEntries) {
        if (provider == null) {
            throw new IllegalArgumentException("provider is required");
        }
        return create(nonce, viewId, provider.toString(), total, exported, skipped,
                exportErrors, state, maxStateBytes, maxStateEntries);
    }

    static Hashtable create(String nonce, String viewId, String provider, int total,
                            int exported, int skipped, int exportErrors, byte[] state) {
        return create(nonce, viewId, provider, total, exported, skipped, exportErrors,
                state, Integer.MAX_VALUE, Integer.MAX_VALUE);
    }

    static Hashtable create(String nonce, String viewId, String provider, int total,
                            int exported, int skipped, int exportErrors, byte[] state,
                            int maxStateBytes, int maxStateEntries) {
        requireText(nonce, "nonce");
        requireText(viewId, "viewId");
        requireText(provider, "provider");
        requireNonNegative(total, "total");
        requireNonNegative(exported, "exported");
        requireNonNegative(skipped, "skipped");
        requireNonNegative(exportErrors, "exportErrors");
        if (state == null) {
            throw new IllegalArgumentException("state is required");
        }
        requirePositive(maxStateBytes, "maxStateBytes");
        requirePositive(maxStateEntries, "maxStateEntries");
        if (state.length > maxStateBytes) {
            throw new IllegalArgumentException("state exceeds configured byte limit");
        }
        if (total > maxStateEntries || exported > maxStateEntries) {
            throw new IllegalArgumentException("state exceeds configured entry limit");
        }

        Hashtable envelope = new Hashtable();
        envelope.put("protocol", PROTOCOL_VERSION);
        envelope.put("type", TYPE);
        envelope.put("nonce", nonce);
        envelope.put("viewId", viewId);
        envelope.put("provider", provider);
        envelope.put("total", Integer.valueOf(total));
        envelope.put("exported", Integer.valueOf(exported));
        envelope.put("skipped", Integer.valueOf(skipped));
        envelope.put("exportErrors", Integer.valueOf(exportErrors));
        envelope.put("state", state.clone());
        return envelope;
    }

    static Hashtable validate(Object payload, String expectedNonce, String expectedViewId,
                              Address expectedProvider, Address source) {
        return validate(payload, expectedNonce, expectedViewId, expectedProvider, source,
                Integer.MAX_VALUE, Integer.MAX_VALUE);
    }

    static Hashtable validate(Object payload, String expectedNonce, String expectedViewId,
                              Address expectedProvider, Address source, int maxStateBytes,
                              int maxStateEntries) {
        if (!(payload instanceof Hashtable)) {
            throw new IllegalArgumentException("state envelope is not a Hashtable");
        }
        if (expectedProvider == null || source == null || !expectedProvider.equals(source)) {
            throw new IllegalArgumentException("state envelope source mismatch");
        }
        Hashtable envelope = (Hashtable) payload;
        requireEquals(PROTOCOL_VERSION, envelope.get("protocol"), "protocol");
        requireEquals(TYPE, envelope.get("type"), "type");
        requireEquals(requireExpectedText(expectedNonce, "expected nonce"),
                requireText(envelope.get("nonce"), "nonce"), "nonce");
        requireEquals(requireExpectedText(expectedViewId, "expected viewId"),
                requireText(envelope.get("viewId"), "viewId"), "viewId");
        requireEquals(expectedProvider.toString(),
                requireText(envelope.get("provider"), "provider"), "provider");

        int total = requireInteger(envelope.get("total"), "total");
        int exported = requireInteger(envelope.get("exported"), "exported");
        int skipped = requireInteger(envelope.get("skipped"), "skipped");
        int exportErrors = requireInteger(envelope.get("exportErrors"), "exportErrors");
        requireNonNegative(total, "total");
        requireNonNegative(exported, "exported");
        requireNonNegative(skipped, "skipped");
        requireNonNegative(exportErrors, "exportErrors");
        if (exported != total || skipped != 0 || exportErrors != 0) {
            throw new IllegalArgumentException("session-state export is incomplete");
        }
        Object state = envelope.get("state");
        if (!(state instanceof byte[])) {
            throw new IllegalArgumentException("state is not a byte array");
        }
        requirePositive(maxStateBytes, "maxStateBytes");
        requirePositive(maxStateEntries, "maxStateEntries");
        if (((byte[]) state).length > maxStateBytes) {
            throw new IllegalArgumentException("state exceeds configured byte limit");
        }
        if (total > maxStateEntries) {
            throw new IllegalArgumentException("state exceeds configured entry limit");
        }
        return create(expectedNonce, expectedViewId, expectedProvider, total, exported, skipped,
                exportErrors, (byte[]) state, maxStateBytes, maxStateEntries);
    }

    static byte[] state(Hashtable envelope) {
        Object state = envelope == null ? null : envelope.get("state");
        if (!(state instanceof byte[])) {
            throw new IllegalArgumentException("state is not a byte array");
        }
        return ((byte[]) state).clone();
    }

    private static int requireInteger(Object value, String field) {
        if (!(value instanceof Integer)) {
            throw new IllegalArgumentException(field + " is not an integer");
        }
        return ((Integer) value).intValue();
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

    private static String requireExpectedText(String value, String field) {
        requireText(value, field);
        return value;
    }

    private static void requireNonNegative(int value, String field) {
        if (value < 0) {
            throw new IllegalArgumentException(field + " is negative");
        }
    }

    private static void requirePositive(int value, String field) {
        if (value < 1) {
            throw new IllegalArgumentException(field + " must be positive");
        }
    }

    private static void requireEquals(Object expected, Object actual, String field) {
        if (!expected.equals(actual)) {
            throw new IllegalArgumentException(field + " mismatch");
        }
    }
}
