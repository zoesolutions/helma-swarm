package helma.swarm;

final class SwarmChannelBootstrapTest {

    private SwarmChannelBootstrapTest() {
    }

    static void register(AllTests.Suite suite) {
        suite.reproduction("strict join properties reject malformed values",
                new AllTests.CheckedRunnable() {
                    public void run() throws Exception {
                        String source = AllTests.source("src/helma/swarm/SwarmJoinPolicy.java");
                        require(source, "swarm.join.strict");
                        require(source, "swarm.join.validationQueryTimeoutSeconds");
                        require(source, "swarm.join.maxConnectTimeoutMillis");
                        require(source, "swarm.join.retryInitialDelayMillis");
                        require(source, "swarm.join.retryMaxDelayMillis");
                        require(source, "swarm.join.maxAttempts");
                        require(source, "swarm.join.minViewSize");
                        require(source, "swarm.join.minViewWaitMillis");
                        require(source, "parseBoolean");
                        require(source, "parseNonNegativeInt");
                        require(source, "minViewSize must be at least 1");
                        require(source, "retryMaxDelayMillis must be greater than or equal");
                    }
                });

        suite.reproduction("strict database URL requires bounded connectTimeout",
                new AllTests.CheckedRunnable() {
                    public void run() throws Exception {
                        String source = AllTests.source("src/helma/swarm/SwarmJoinPolicy.java");
                        String method = AllTests.methodBody(source,
                                "static int validateMysqlConnectTimeout(String url, int maximum)");
                        require(method, "jdbc:mysql:");
                        require(method, "connectTimeout");
                        require(method, "timeout <= 0");
                        require(method, "timeout > maximum");
                        require(method, "duplicate");
                    }
                });

        suite.reproduction("join and lifecycle getters remain side-effect-free",
                new AllTests.CheckedRunnable() {
                    public void run() throws Exception {
                        String source = AllTests.source("src/helma/swarm/ChannelUtils.java");
                        assertReadOnly(source,
                                "public static boolean isJoinStateAvailable(Application app)");
                        assertReadOnly(source,
                                "public static String getJoinStatus(Application app)");
                        assertReadOnly(source,
                                "public static int getJoinAttempts(Application app)");
                        assertReadOnly(source,
                                "public static String getJoinLastError(Application app)");
                        String lifecycle = AllTests.source("src/helma/swarm/SwarmLifecycle.java");
                        require(lifecycle, "getControlService()");
                        require(lifecycle, "getCapability()");
                        require(lifecycle, "getMemberRole()");
                    }
                });
    }

    private static void assertReadOnly(String source, String signature) {
        String method = AllTests.methodBody(source, signature);
        require(method, "getExistingLifecycle(app)");
        AllTests.assertTrue(!method.contains("getAdapter(app)"),
                signature + " must not create an adapter");
        AllTests.assertTrue(!method.contains("new SwarmChannelBootstrap("),
                signature + " must not start bootstrap");
    }

    private static void require(String source, String token) {
        AllTests.assertTrue(source.contains(token), "missing required behavior: " + token);
    }
}
