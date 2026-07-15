package helma.swarm;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import helma.framework.core.Application;
import helma.util.ResourceProperties;
import org.jgroups.Channel;
import org.jgroups.JChannel;
import org.jgroups.blocks.PullPushAdapter;

public final class JdbcPingRecoveryIT {

    private static final String INITIALIZE_SQL = "CREATE TABLE IF NOT EXISTS JGROUPSPING "
            + "(own_addr varchar(200) NOT NULL, cluster_name varchar(200) NOT NULL, "
            + "ping_data varbinary(5000) DEFAULT NULL, PRIMARY KEY (own_addr, cluster_name))";

    private JdbcPingRecoveryIT() {
    }

    public static void main(String[] args) throws Exception {
        if (!"1".equals(System.getenv("HELMA_SWARM_IT"))) {
            System.out.println("SKIP JdbcPingRecoveryIT: set HELMA_SWARM_IT=1 after docker compose is healthy");
            return;
        }

        String directUrl = env("IT_DB_DIRECT_URL",
                "jdbc:mysql://127.0.0.1:13307/swarm?connectTimeout=2000&socketTimeout=2000&useSSL=false&allowPublicKeyRetrieval=true");
        String proxyUrl = env("IT_DB_PROXY_URL",
                "jdbc:mysql://127.0.0.1:13306/swarm?connectTimeout=2000&socketTimeout=2000&useSSL=false&allowPublicKeyRetrieval=true");
        String user = env("IT_DB_USER", "swarm");
        String password = env("IT_DB_PASSWORD", "swarm-test-only");
        String toxiproxy = env("IT_TOXIPROXY_URL", "http://127.0.0.1:18474");
        String cluster = "jdbc-ping-recovery-" + UUID.randomUUID().toString();

        Class.forName("com.mysql.cj.jdbc.Driver");
        verifyDatabase(directUrl, user, password);
        setProxyEnabled(toxiproxy, true);
        verifyDatabase(proxyUrl, user, password);

        Channel healthy = null;
        SwarmLifecycle lifecycle = null;
        ProcessShutdown shutdown = new ProcessShutdown(false);
        BlockingRetryChannelFactory channels =
                new BlockingRetryChannelFactory(proxyUrl, user, password, cluster);
        ObservingSleeper sleeper = new ObservingSleeper(channels);
        BootstrapRun run = new BootstrapRun();
        Thread bootstrapThread = null;
        try {
            healthy = connect(directUrl, user, password, 17800, cluster);
            Application app = strictApplication(cluster);
            SwarmJoinPolicy policy = SwarmJoinPolicy.from(app.getProperties());
            lifecycle = new SwarmLifecycle(policy);
            SwarmChannelBootstrap bootstrap = new SwarmChannelBootstrap(
                    app, lifecycle, new DirectDatabaseProbe(directUrl, user, password),
                    channels, sleeper, shutdown);

            setProxyEnabled(toxiproxy, false);
            run.bootstrap = bootstrap;
            bootstrapThread = new Thread(run, "jdbc-ping-bootstrap-it");
            bootstrapThread.start();

            require(channels.awaitSecondAttempt(15, TimeUnit.SECONDS),
                    "bootstrap did not discard the blocked JDBC_PING attempt");
            Channel first = channels.firstChannel();
            require(first != null, "bootstrap did not create its first channel");
            require(sleeper.observedConnectedSingleton(),
                    "blocked JDBC_PING attempt was not observed as a connected singleton");
            require(!first.isOpen(),
                    "bootstrap entered its second attempt before closing the singleton channel");
            System.out.println("OBSERVED production bootstrap closed connected singleton before retry");

            setProxyEnabled(toxiproxy, true);
            channels.releaseSecondAttempt();
            bootstrapThread.join(15000L);
            require(!bootstrapThread.isAlive(), "production bootstrap did not recover in time");
            if (run.failure != null) {
                throw new AssertionError("production bootstrap failed", run.failure);
            }
            require(run.adapter != null, "production bootstrap returned no adapter");
            Channel recovered = (Channel) run.adapter.getTransport();
            require(channels.createdCount() == 2,
                    "production bootstrap did not recover on exactly one fresh retry");
            require(recovered != first, "production bootstrap reused the failed channel");
            require(recovered.isConnected() && recovered.getView().size() >= 2,
                    "fresh production channel did not discover the healthy member");
            require(lifecycle.getChannel() == recovered,
                    "lifecycle did not retain the recovered channel before publication");
            require(lifecycle.publishPreReady(run.adapter),
                    "recovered adapter could not be atomically published PRE_READY");
            System.out.println("OBSERVED production bootstrap recovered fresh channel viewSize="
                    + recovered.getView().size() + " view=" + recovered.getView());
        } finally {
            channels.releaseSecondAttempt();
            setProxyEnabled(toxiproxy, true);
            shutdown.signalShutdown();
            if (lifecycle != null) {
                lifecycle.stop();
            }
            if (bootstrapThread != null) {
                bootstrapThread.join(5000L);
            }
            close(healthy);
        }
    }

    private static Application strictApplication(String cluster) {
        Application app = new Application(cluster);
        try {
            Field props = Application.class.getDeclaredField("props");
            props.setAccessible(true);
            props.set(app, new ResourceProperties());
            Field eventLogName = Application.class.getDeclaredField("eventLogName");
            eventLogName.setAccessible(true);
            eventLogName.set(app, "jdbc-ping-recovery-it");
        } catch (Exception failure) {
            throw new AssertionError("could not initialize Application properties", failure);
        }
        app.getProperties().setProperty("swarm.name", cluster);
        app.getProperties().setProperty("swarm.join.strict", "true");
        app.getProperties().setProperty("swarm.join.retryInitialDelayMillis", "0");
        app.getProperties().setProperty("swarm.join.retryMaxDelayMillis", "0");
        app.getProperties().setProperty("swarm.join.maxAttempts", "0");
        app.getProperties().setProperty("swarm.join.minViewSize", "2");
        app.getProperties().setProperty("swarm.join.minViewWaitMillis", "500");
        app.getProperties().setProperty("swarm.session.stateProviderMode", "strict");
        app.getProperties().setProperty("swarm.session.memberRole", "session");
        app.getProperties().setProperty("sessionManagerImpl",
                "helma.swarm.SwarmSessionManager");
        return app;
    }

    private static Channel connect(String url, String user, String password, int port,
                                   String cluster) throws Exception {
        JChannel channel = new JChannel(stack(url, user, password, port));
        channel.connect(cluster + "_swarm");
        return channel;
    }

    private static String stack(String url, String user, String password, int port) {
        return "TCP(bind_addr=127.0.0.1;bind_port=" + port + ";port_range=0;loopback=true):"
                + "JDBC_PING(connection_driver=com.mysql.cj.jdbc.Driver;connection_url=" + url
                + ";connection_username=" + user + ";connection_password=" + password
                + ";initialize_sql=\"" + INITIALIZE_SQL + "\""
                + ";insert_single_sql=\"INSERT INTO JGROUPSPING (own_addr, cluster_name, ping_data) values (?, ?, ?)\""
                + ";delete_single_sql=\"DELETE FROM JGROUPSPING WHERE own_addr=? AND cluster_name=?\""
                + ";select_all_pingdata_sql=\"SELECT ping_data FROM JGROUPSPING WHERE cluster_name=?\"):"
                + "MERGE2(min_interval=1000;max_interval=2000):"
                + "FD_SOCK:VERIFY_SUSPECT(timeout=500):"
                + "pbcast.NAKACK:UNICAST:pbcast.STABLE:pbcast.GMS(join_timeout=2000;print_local_addr=false):"
                + "FRAG2:pbcast.STATE_TRANSFER";
    }

    private static void verifyDatabase(String url, String user, String password) throws Exception {
        Connection connection = DriverManager.getConnection(url, user, password);
        try {
            Statement statement = connection.createStatement();
            try {
                statement.execute(INITIALIZE_SQL);
                statement.execute("SELECT 1");
            } finally {
                statement.close();
            }
        } finally {
            connection.close();
        }
    }

    private static void setProxyEnabled(String baseUrl, boolean enabled) throws IOException {
        HttpURLConnection connection = (HttpURLConnection) new URL(
                baseUrl + "/proxies/mariadb").openConnection();
        connection.setRequestMethod("POST");
        connection.setRequestProperty("Content-Type", "application/json");
        connection.setConnectTimeout(2000);
        connection.setReadTimeout(2000);
        connection.setDoOutput(true);
        OutputStream output = connection.getOutputStream();
        try {
            output.write(("{\"enabled\":" + enabled + "}").getBytes("UTF-8"));
        } finally {
            output.close();
        }
        int status = connection.getResponseCode();
        connection.disconnect();
        if (status < 200 || status >= 300) {
            throw new IOException("Toxiproxy returned HTTP " + status);
        }
    }

    private static void close(Channel channel) {
        if (channel != null) {
            channel.close();
        }
    }

    private static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.length() == 0 ? fallback : value;
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }

    private static final class DirectDatabaseProbe implements DatabaseProbe {
        private final String url;
        private final String user;
        private final String password;

        DirectDatabaseProbe(String url, String user, String password) {
            this.url = url;
            this.user = user;
            this.password = password;
        }

        public void validate(Application ignored, SwarmJoinPolicy policy) throws Exception {
            verifyDatabase(url, user, password);
        }
    }

    private static final class BlockingRetryChannelFactory implements ChannelFactory {
        private final String url;
        private final String user;
        private final String password;
        private final String cluster;
        private final CountDownLatch secondAttempt = new CountDownLatch(1);
        private final CountDownLatch releaseSecond = new CountDownLatch(1);
        private final List channels = new ArrayList();
        private int attempts;

        BlockingRetryChannelFactory(String url, String user, String password,
                                    String cluster) {
            this.url = url;
            this.user = user;
            this.password = password;
            this.cluster = cluster;
        }

        public Channel create(Application ignored) throws Exception {
            int attempt;
            synchronized (this) {
                attempts++;
                attempt = attempts;
            }
            if (attempt == 2) {
                secondAttempt.countDown();
                if (!releaseSecond.await(15, TimeUnit.SECONDS)) {
                    throw new AssertionError("second bootstrap attempt was not released");
                }
            } else if (attempt > 2) {
                throw new AssertionError("unexpected bootstrap attempt " + attempt);
            }
            Channel channel = new JChannel(stack(url, user, password,
                    17809 + attempt));
            synchronized (this) {
                channels.add(channel);
            }
            return channel;
        }

        boolean awaitSecondAttempt(long timeout, TimeUnit unit) throws InterruptedException {
            return secondAttempt.await(timeout, unit);
        }

        void releaseSecondAttempt() {
            releaseSecond.countDown();
        }

        synchronized Channel firstChannel() {
            return channels.isEmpty() ? null : (Channel) channels.get(0);
        }

        synchronized Channel latestChannel() {
            return channels.isEmpty() ? null : (Channel) channels.get(channels.size() - 1);
        }

        synchronized int createdCount() {
            return channels.size();
        }
    }

    private static final class ObservingSleeper implements Sleeper {
        private final BlockingRetryChannelFactory channels;
        private volatile boolean connectedSingleton;

        ObservingSleeper(BlockingRetryChannelFactory channels) {
            this.channels = channels;
        }

        public void sleep(long delayMillis) throws InterruptedException {
            Channel channel = channels.latestChannel();
            if (channel != null && channel.isConnected() && channel.getView() != null
                    && channel.getView().size() == 1) {
                connectedSingleton = true;
            }
            Thread.sleep(delayMillis);
        }

        boolean observedConnectedSingleton() {
            return connectedSingleton;
        }
    }

    private static final class BootstrapRun implements Runnable {
        private SwarmChannelBootstrap bootstrap;
        private volatile PullPushAdapter adapter;
        private volatile Throwable failure;

        public void run() {
            try {
                adapter = bootstrap.bootstrap();
            } catch (Throwable caught) {
                failure = caught;
            }
        }
    }
}
