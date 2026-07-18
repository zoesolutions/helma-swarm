package helma.swarm;

import helma.framework.core.Application;

final class LegacyBehaviorTest {

    private LegacyBehaviorTest() {
    }

    static void register(AllTests.Suite suite) {
        suite.baseline("legacy status inspection does not create a channel",
                new AllTests.CheckedRunnable() {
                    public void run() {
                        Application app = new Application("legacy-status-baseline");
                        AllTests.assertTrue(!ChannelUtils.isConnected(app),
                                "status inspection reported a connection without an adapter");
                        AllTests.assertEquals(Integer.valueOf(0),
                                Integer.valueOf(ChannelUtils.getViewSize(app)),
                                "status inspection reported members without an adapter");
                        AllTests.assertTrue(!ChannelUtils.adapters.containsKey(app),
                                "status inspection created an adapter");
                    }
                });
        suite.baseline("legacy stop is idempotent without an adapter",
                new AllTests.CheckedRunnable() {
                    public void run() {
                        Application app = new Application("legacy-stop-baseline");
                        ChannelUtils.stopAdapter(app);
                        ChannelUtils.stopAdapter(app);
                        AllTests.assertTrue(!ChannelUtils.adapters.containsKey(app),
                                "idempotent stop retained an adapter");
                    }
                });
        suite.baseline("legacy wire routing constants remain stable",
                new AllTests.CheckedRunnable() {
                    public void run() {
                        AllTests.assertEquals(Integer.valueOf(1), ChannelUtils.CACHE,
                                "cache listener id changed");
                        AllTests.assertEquals(Integer.valueOf(2), ChannelUtils.IDGEN,
                                "id-generator listener id changed");
                        AllTests.assertEquals(Integer.valueOf(0),
                                Integer.valueOf(SwarmSessionManager.TOUCH),
                                "session touch operation changed");
                        AllTests.assertEquals(Integer.valueOf(1),
                                Integer.valueOf(SwarmSessionManager.DISCARD),
                                "session discard operation changed");
                    }
                });
    }
}
