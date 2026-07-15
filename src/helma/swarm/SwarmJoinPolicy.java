/*
 * Helma License Notice
 *
 * The contents of this file are subject to the Helma License
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License.
 */
package helma.swarm;

import java.util.Properties;

public final class SwarmJoinPolicy {

    public static enum StateProviderMode {
        LEGACY, STRICT
    }

    public static enum MemberRole {
        LEGACY, SESSION, NON_SESSION
    }

    static final String STRICT = "swarm.join.strict";
    static final String DB_SOURCE = "swarm.join.dbSource";
    static final String VALIDATION_QUERY = "swarm.join.validationQuery";
    static final String VALIDATION_QUERY_TIMEOUT =
            "swarm.join.validationQueryTimeoutSeconds";
    static final String MAX_CONNECT_TIMEOUT = "swarm.join.maxConnectTimeoutMillis";
    static final String RETRY_INITIAL_DELAY = "swarm.join.retryInitialDelayMillis";
    static final String RETRY_MAX_DELAY = "swarm.join.retryMaxDelayMillis";
    static final String MAX_ATTEMPTS = "swarm.join.maxAttempts";
    static final String MIN_VIEW_SIZE = "swarm.join.minViewSize";
    static final String MIN_VIEW_WAIT = "swarm.join.minViewWaitMillis";
    static final String STATE_PROVIDER_MODE = "swarm.session.stateProviderMode";
    static final String MEMBER_ROLE = "swarm.session.memberRole";
    static final String DISCOVERY_TIMEOUT = "swarm.session.discoveryTimeoutMillis";
    static final String DISCOVERY_RETRY_DELAY =
            "swarm.session.discoveryRetryDelayMillis";
    static final String STATE_TRANSFER_TIMEOUT =
            "swarm.session.stateTransferTimeoutMillis";
    static final String STATE_MAX_BYTES = "swarm.session.stateMaxBytes";
    static final String STATE_MAX_ENTRIES = "swarm.session.stateMaxEntries";
    static final String BOOTSTRAP_BUFFER_MAX =
            "swarm.session.bootstrapBufferMaxMessages";
    static final String BOOTSTRAP_BUFFER_MAX_BYTES =
            "swarm.session.bootstrapBufferMaxBytes";
    static final String BOOTSTRAP_BUFFER_MAX_ENTRIES =
            "swarm.session.bootstrapBufferMaxEntries";
    static final String SESSION_MANAGER_IMPL = "sessionManagerImpl";
    static final String SWARM_SESSION_MANAGER = "helma.swarm.SwarmSessionManager";
    static final String SWARM_NON_SESSION_MANAGER =
            "helma.swarm.SwarmNonSessionManager";

    private final boolean strict;
    private final String dbSource;
    private final String validationQuery;
    private final int validationQueryTimeoutSeconds;
    private final int maxConnectTimeoutMillis;
    private final int retryInitialDelayMillis;
    private final int retryMaxDelayMillis;
    private final int maxAttempts;
    private final int minViewSize;
    private final int minViewWaitMillis;
    private final StateProviderMode stateProviderMode;
    private final MemberRole memberRole;
    private final int discoveryTimeoutMillis;
    private final int discoveryRetryDelayMillis;
    private final int stateTransferTimeoutMillis;
    private final int stateMaxBytes;
    private final int stateMaxEntries;
    private final int bootstrapBufferMaxMessages;
    private final int bootstrapBufferMaxBytes;
    private final int bootstrapBufferMaxEntries;

    private SwarmJoinPolicy(boolean strict, String dbSource, String validationQuery,
                            int validationQueryTimeoutSeconds,
                            int maxConnectTimeoutMillis, int retryInitialDelayMillis,
                            int retryMaxDelayMillis, int maxAttempts, int minViewSize,
                            int minViewWaitMillis, StateProviderMode stateProviderMode,
                            MemberRole memberRole, int discoveryTimeoutMillis,
                            int discoveryRetryDelayMillis, int stateTransferTimeoutMillis,
                            int stateMaxBytes, int stateMaxEntries,
                            int bootstrapBufferMaxMessages, int bootstrapBufferMaxBytes,
                            int bootstrapBufferMaxEntries) {
        this.strict = strict;
        this.dbSource = dbSource;
        this.validationQuery = validationQuery;
        this.validationQueryTimeoutSeconds = validationQueryTimeoutSeconds;
        this.maxConnectTimeoutMillis = maxConnectTimeoutMillis;
        this.retryInitialDelayMillis = retryInitialDelayMillis;
        this.retryMaxDelayMillis = retryMaxDelayMillis;
        this.maxAttempts = maxAttempts;
        this.minViewSize = minViewSize;
        this.minViewWaitMillis = minViewWaitMillis;
        this.stateProviderMode = stateProviderMode;
        this.memberRole = memberRole;
        this.discoveryTimeoutMillis = discoveryTimeoutMillis;
        this.discoveryRetryDelayMillis = discoveryRetryDelayMillis;
        this.stateTransferTimeoutMillis = stateTransferTimeoutMillis;
        this.stateMaxBytes = stateMaxBytes;
        this.stateMaxEntries = stateMaxEntries;
        this.bootstrapBufferMaxMessages = bootstrapBufferMaxMessages;
        this.bootstrapBufferMaxBytes = bootstrapBufferMaxBytes;
        this.bootstrapBufferMaxEntries = bootstrapBufferMaxEntries;
    }

    static SwarmJoinPolicy from(Properties properties) {
        boolean strict = parseBoolean(properties, STRICT, false);
        if (!strict) {
            return legacy();
        }

        String dbSource = requiredText(properties, DB_SOURCE, "iiefs");
        String validationQuery = requiredText(properties, VALIDATION_QUERY, "SELECT 1");
        if (!"SELECT 1".equalsIgnoreCase(validationQuery)) {
            throw invalid(VALIDATION_QUERY, "must be SELECT 1");
        }
        int validationQueryTimeoutSeconds = parseNonNegativeInt(properties,
                VALIDATION_QUERY_TIMEOUT, 2);
        int maxConnectTimeoutMillis = parseNonNegativeInt(properties,
                MAX_CONNECT_TIMEOUT, 2000);
        int retryInitialDelayMillis = parseNonNegativeInt(properties,
                RETRY_INITIAL_DELAY, 1000);
        int retryMaxDelayMillis = parseNonNegativeInt(properties,
                RETRY_MAX_DELAY, 5000);
        int maxAttempts = parseNonNegativeInt(properties, MAX_ATTEMPTS, 1);
        int minViewSize = parseNonNegativeInt(properties, MIN_VIEW_SIZE, 1);
        int minViewWaitMillis = parseNonNegativeInt(properties, MIN_VIEW_WAIT, 10000);
        StateProviderMode stateProviderMode = parseStateProviderMode(properties);
        MemberRole memberRole = parseMemberRole(properties);
        int discoveryTimeoutMillis = parseNonNegativeInt(properties,
                DISCOVERY_TIMEOUT, 2000);
        int discoveryRetryDelayMillis = parseNonNegativeInt(properties,
                DISCOVERY_RETRY_DELAY, 500);
        int stateTransferTimeoutMillis = parseNonNegativeInt(properties,
                STATE_TRANSFER_TIMEOUT, 5000);
        int stateMaxBytes = parseNonNegativeInt(properties, STATE_MAX_BYTES, 67108864);
        int stateMaxEntries = parseNonNegativeInt(properties, STATE_MAX_ENTRIES, 100000);
        int bootstrapBufferMaxMessages = parseNonNegativeInt(properties,
                BOOTSTRAP_BUFFER_MAX, 10000);
        int bootstrapBufferMaxBytes = parseNonNegativeInt(properties,
                BOOTSTRAP_BUFFER_MAX_BYTES, 16777216);
        int bootstrapBufferMaxEntries = parseNonNegativeInt(properties,
                BOOTSTRAP_BUFFER_MAX_ENTRIES, 100000);

        if (minViewSize < 1) {
            throw invalid(MIN_VIEW_SIZE, "minViewSize must be at least 1");
        }
        if (retryMaxDelayMillis < retryInitialDelayMillis) {
            throw invalid(RETRY_MAX_DELAY,
                    "retryMaxDelayMillis must be greater than or equal to retryInitialDelayMillis");
        }
        if (bootstrapBufferMaxMessages < 1) {
            throw invalid(BOOTSTRAP_BUFFER_MAX,
                    "bootstrapBufferMaxMessages must be greater than 0");
        }
        requirePositive(validationQueryTimeoutSeconds, VALIDATION_QUERY_TIMEOUT);
        requirePositive(stateMaxBytes, STATE_MAX_BYTES);
        requirePositive(stateMaxEntries, STATE_MAX_ENTRIES);
        requirePositive(bootstrapBufferMaxBytes, BOOTSTRAP_BUFFER_MAX_BYTES);
        requirePositive(bootstrapBufferMaxEntries, BOOTSTRAP_BUFFER_MAX_ENTRIES);
        if (stateProviderMode != StateProviderMode.STRICT) {
            throw invalid(STATE_PROVIDER_MODE,
                    "STRICT join requires stateProviderMode=strict");
        }
        if (memberRole == MemberRole.LEGACY) {
            throw invalid(MEMBER_ROLE,
                    "STRICT join requires memberRole=session or non-session");
        }
        String sessionManagerImpl = requiredText(properties, SESSION_MANAGER_IMPL,
                "helma.framework.core.SessionManager");
        if (memberRole == MemberRole.SESSION
                && !SWARM_SESSION_MANAGER.equals(sessionManagerImpl)) {
            throw invalid(SESSION_MANAGER_IMPL,
                    "memberRole=session requires helma.swarm.SwarmSessionManager");
        }
        if (memberRole == MemberRole.NON_SESSION
                && !SWARM_NON_SESSION_MANAGER.equals(sessionManagerImpl)) {
            throw invalid(SESSION_MANAGER_IMPL,
                    "memberRole=non-session requires helma.swarm.SwarmNonSessionManager");
        }

        return new SwarmJoinPolicy(true, dbSource, validationQuery,
                validationQueryTimeoutSeconds, maxConnectTimeoutMillis,
                retryInitialDelayMillis, retryMaxDelayMillis, maxAttempts,
                minViewSize, minViewWaitMillis, stateProviderMode, memberRole,
                discoveryTimeoutMillis, discoveryRetryDelayMillis,
                stateTransferTimeoutMillis, stateMaxBytes, stateMaxEntries,
                bootstrapBufferMaxMessages, bootstrapBufferMaxBytes,
                bootstrapBufferMaxEntries);
    }

    static boolean strictRequested(Properties properties) {
        return parseBoolean(properties, STRICT, false);
    }

    static SwarmJoinPolicy strictDefaultsForInvalidConfiguration() {
        return new SwarmJoinPolicy(true, "iiefs", "SELECT 1", 2, 2000,
                1000, 5000, 0, 1, 10000, StateProviderMode.STRICT,
                MemberRole.LEGACY, 2000, 500, 5000, 67108864, 100000,
                10000, 16777216, 100000);
    }

    private static SwarmJoinPolicy legacy() {
        return new SwarmJoinPolicy(false, "iiefs", "SELECT 1", 2, 2000,
                1000, 5000, 1, 1, 10000, StateProviderMode.LEGACY,
                MemberRole.LEGACY, 2000, 500, 5000, 67108864, 100000,
                10000, 16777216, 100000);
    }

    static int validateMysqlConnectTimeout(String url, int maximum) {
        if (url == null || !url.toLowerCase().startsWith("jdbc:mysql:")) {
            throw new IllegalArgumentException(
                    "database URL must use jdbc:mysql: and contain a bounded connectTimeout");
        }
        int query = url.indexOf('?');
        String[] parameters = query < 0 ? new String[0]
                : url.substring(query + 1).split("&", -1);
        String configured = null;
        for (int i = 0; i < parameters.length; i++) {
            String parameter = parameters[i];
            int equals = parameter.indexOf('=');
            String name = equals < 0 ? parameter : parameter.substring(0, equals);
            if ("connectTimeout".equalsIgnoreCase(name)) {
                if (configured != null) {
                    throw new IllegalArgumentException(
                            "database URL contains duplicate connectTimeout values");
                }
                configured = equals < 0 ? "" : parameter.substring(equals + 1);
            }
        }
        if (configured == null || configured.length() == 0) {
            throw new IllegalArgumentException(
                    "database URL is missing required connectTimeout");
        }
        int timeout;
        try {
            timeout = Integer.parseInt(configured);
        } catch (NumberFormatException badTimeout) {
            throw new IllegalArgumentException(
                    "database URL connectTimeout must be an integer");
        }
        if (timeout <= 0) {
            throw new IllegalArgumentException(
                    "database URL connectTimeout must be greater than 0");
        }
        if (timeout > maximum) {
            throw new IllegalArgumentException(
                    "database URL connectTimeout exceeds configured maximum");
        }
        return timeout;
    }

    private static boolean parseBoolean(Properties properties, String name,
                                        boolean defaultValue) {
        String value = properties.getProperty(name);
        if (value == null) {
            return defaultValue;
        }
        value = value.trim();
        if ("true".equalsIgnoreCase(value)) {
            return true;
        }
        if ("false".equalsIgnoreCase(value)) {
            return false;
        }
        throw invalid(name, "must be true or false");
    }

    private static int parseNonNegativeInt(Properties properties, String name,
                                           int defaultValue) {
        String value = properties.getProperty(name);
        if (value == null) {
            return defaultValue;
        }
        final int parsed;
        try {
            parsed = Integer.parseInt(value.trim());
        } catch (NumberFormatException badInteger) {
            throw invalid(name, "must be an integer");
        }
        if (parsed < 0) {
            throw invalid(name, "must not be negative");
        }
        return parsed;
    }

    private static String requiredText(Properties properties, String name,
                                       String defaultValue) {
        String value = properties.getProperty(name, defaultValue).trim();
        if (value.length() == 0) {
            throw invalid(name, "must not be empty");
        }
        return value;
    }

    private static void requirePositive(int value, String property) {
        if (value < 1) {
            throw invalid(property, "must be greater than 0");
        }
    }

    private static StateProviderMode parseStateProviderMode(Properties properties) {
        String value = properties.getProperty(STATE_PROVIDER_MODE, "legacy").trim();
        if ("legacy".equalsIgnoreCase(value)) {
            return StateProviderMode.LEGACY;
        }
        if ("strict".equalsIgnoreCase(value)) {
            return StateProviderMode.STRICT;
        }
        throw invalid(STATE_PROVIDER_MODE, "must be legacy or strict");
    }

    private static MemberRole parseMemberRole(Properties properties) {
        String value = properties.getProperty(MEMBER_ROLE, "legacy").trim();
        if ("legacy".equalsIgnoreCase(value)) {
            return MemberRole.LEGACY;
        }
        if ("session".equalsIgnoreCase(value)) {
            return MemberRole.SESSION;
        }
        if ("non-session".equalsIgnoreCase(value)) {
            return MemberRole.NON_SESSION;
        }
        throw invalid(MEMBER_ROLE, "must be legacy, session, or non-session");
    }

    private static IllegalArgumentException invalid(String property, String reason) {
        return new IllegalArgumentException(property + " " + reason);
    }

    public boolean isStrict() {
        return strict;
    }

    public String getDbSource() {
        return dbSource;
    }

    public String getValidationQuery() {
        return validationQuery;
    }

    public int getValidationQueryTimeoutSeconds() {
        return validationQueryTimeoutSeconds;
    }

    public int getMaxConnectTimeoutMillis() {
        return maxConnectTimeoutMillis;
    }

    public int getRetryInitialDelayMillis() {
        return retryInitialDelayMillis;
    }

    public int getRetryMaxDelayMillis() {
        return retryMaxDelayMillis;
    }

    public int getMaxAttempts() {
        return maxAttempts;
    }

    public int getMinViewSize() {
        return minViewSize;
    }

    public int getMinViewWaitMillis() {
        return minViewWaitMillis;
    }

    public StateProviderMode getStateProviderMode() {
        return stateProviderMode;
    }

    public MemberRole getMemberRole() {
        return memberRole;
    }

    public int getDiscoveryTimeoutMillis() {
        return discoveryTimeoutMillis;
    }

    public int getDiscoveryRetryDelayMillis() {
        return discoveryRetryDelayMillis;
    }

    public int getStateTransferTimeoutMillis() {
        return stateTransferTimeoutMillis;
    }

    public int getStateMaxBytes() {
        return stateMaxBytes;
    }

    public int getStateMaxEntries() {
        return stateMaxEntries;
    }

    public int getBootstrapBufferMaxMessages() {
        return bootstrapBufferMaxMessages;
    }

    public int getBootstrapBufferMaxBytes() {
        return bootstrapBufferMaxBytes;
    }

    public int getBootstrapBufferMaxEntries() {
        return bootstrapBufferMaxEntries;
    }
}
