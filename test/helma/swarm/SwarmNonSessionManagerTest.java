package helma.swarm;

final class SwarmNonSessionManagerTest {

    private SwarmNonSessionManagerTest() {
    }

    static void register(AllTests.Suite suite) {
        suite.baseline("legacy channel status helpers are side-effect-free",
                new AllTests.CheckedRunnable() {
                    public void run() throws Exception {
                        String source = AllTests.source("src/helma/swarm/ChannelUtils.java");
                        assertReadOnly(source, "public static boolean isConnected(Application app)");
                        assertReadOnly(source, "public static int getViewSize(Application app)");
                        assertReadOnly(source, "public static String getView(Application app)");
                        assertReadOnly(source, "public static boolean isMaster(Application app)");
                    }
                });

        suite.reproduction("session state never targets an arbitrary coordinator",
                new AllTests.CheckedRunnable() {
                    public void run() throws Exception {
                        String source = AllTests.source("src/helma/swarm/SwarmSessionManager.java");
                        String init = AllTests.methodBody(source, "public void init(Application app)");
                        int legacyGuard = init.indexOf("if (!strictMode)");
                        int legacyState = init.indexOf("getState(null");
                        int legacyReturn = init.indexOf("return;", legacyState);
                        AllTests.assertTrue(legacyGuard >= 0 && legacyState > legacyGuard
                                        && legacyReturn > legacyState,
                                "getState(null) must remain confined to the compatible LEGACY branch");
                        String strict = AllTests.methodBody(source,
                                "private void bootstrapStrictSessions(ScriptingEngineInterface loadEngine)");
                        AllTests.assertTrue(!strict.contains("getState(null")
                                        && strict.contains("requestState(provider"),
                                "STRICT bootstrap must request state from an explicit provider");
                    }
                });

        suite.reproduction("session readiness getters remain side-effect-free",
                new AllTests.CheckedRunnable() {
                    public void run() throws Exception {
                        String source = AllTests.source("src/helma/swarm/ChannelUtils.java");
                        assertSessionReadOnly(source,
                                "public static boolean isSessionStateAvailable(Application app)");
                        assertSessionReadOnly(source,
                                "public static boolean isSessionStateInitialized(Application app)");
                        assertSessionReadOnly(source,
                                "public static String getSessionStateStatus(Application app)");
                        assertSessionReadOnly(source,
                                "public static String getSessionStateProvider(Application app)");
                        assertSessionReadOnly(source,
                                "public static String getKnownSessionStateProviders(Application app)");
                        assertSessionReadOnly(source,
                                "public static int getLastReceivedStateSessionCount(Application app)");
                        assertSessionReadOnly(source,
                                "public static String getSessionStateLastError(Application app)");
                        assertSessionReadOnly(source,
                                "public static boolean isControlProtocolComplete(Application app)");
                    }
                });
    }

    private static void assertReadOnly(String source, String signature) {
        String body = AllTests.methodBody(source, signature);
        AllTests.assertTrue(body.contains("getExistingChannel(app)"),
                signature + " must inspect only an existing channel");
        AllTests.assertTrue(!body.contains("getAdapter(app)"),
                signature + " must not create or connect an adapter");
    }

    private static void assertSessionReadOnly(String source, String signature) {
        String body = AllTests.methodBody(source, signature);
        AllTests.assertTrue(body.contains("getExistingSessionManager(app)"),
                signature + " must inspect only the installed manager");
        AllTests.assertTrue(!body.contains("getAdapter(app)"),
                signature + " must not create or connect an adapter");
    }
}
