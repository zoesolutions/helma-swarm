package helma.swarm;

import helma.framework.core.Application;
import helma.framework.core.SessionManager;

public class SwarmNonSessionManager extends SessionManager {

    public void init(Application app) {
        super.init(app);
        try {
            ChannelUtils.getAdapter(app);
            ChannelUtils.commitNonSessionReady(app, this);
        } catch (Exception failure) {
            app.logError("HelmaSwarm: Error starting non-session swarm member", failure);
        }
    }

    public void shutdown() {
        super.shutdown();
        ChannelUtils.stopAdapter(app);
    }
}
