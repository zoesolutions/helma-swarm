package helma.swarm;

import helma.framework.core.Application;
import org.jgroups.Channel;
import org.jgroups.JChannel;
import org.jgroups.blocks.PullPushAdapter;

import java.io.IOException;
import java.io.OutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public final class JdbcPingRecoveryIT {

    static final int CANDIDATE_BIND_PORT = 17809;
    private static final String INITIALIZE_SQL =
            "CREATE TABLE IF NOT EXISTS JGROUPSPING "
            + "(own_addr varchar(200) NOT NULL, cluster_name varchar(200) NOT NULL, "
            + "ping_data varbinary(5000) DEFAULT NULL, "
            + "PRIMARY KEY (own_addr, cluster_name))";

    private JdbcPingRecoveryIT() {
    }

    public static void main(String[] args) throws Exception {
        if (!"1".equals(System.getenv("HELMA_SWARM_IT"))) {
            throw new IllegalStateException(
                    "HELMA_SWARM_IT=1 is required; integration tests must never report SKIP as success");
        }

        String directUrl = env("IT_DB_DIRECT_URL",
                "jdbc:mysql://127.0.0.1:13307/swarm?connectTimeout=2000&socketTimeout=2000&useSSL=false&allowPublicKeyRetrieval=true");
        String proxyUrl = env("IT_DB_PROXY_URL",
                "jdbc:mysql://127.0.0.1:13306/swarm?connectTimeout=2000&socketTimeout=2000&useSSL=false&allowPublicKeyRetrieval=true");
        String user = env("IT_DB_USER", "swarm");
        String password = env("IT_DB_PASSWORD", "swarm-test-only");
        String toxiproxy = env("IT_TOXIPROXY_URL", "http://127.0.0.1:18474");
        final String cluster = "jdbc-ping-recovery-" + UUID.randomUUID().toString();

        Class.forName("com.mysql.cj.jdbc.Driver");
        verifyDatabase(directUrl, user, password);
        setProxyEnabled(toxiproxy, true);
        verifyDatabase(proxyUrl, user, password);

        Channel healthy = null;
        BootstrapState state = new BootstrapState();
        BlockingChannelFactory channels =
                new BlockingChannelFactory(proxyUrl, user, password, cluster);
        ObservingScheduler scheduler = new ObservingScheduler(channels);
        BootstrapRun run = new BootstrapRun();
        Thread bootstrapThread = null;
        try {
            healthy = connect(directUrl, user, password, 17800, cluster);
            StartupChannelBootstrap bootstrap = new StartupChannelBootstrap(
                    new StartupChannelBootstrap.Dependencies(
                            channels, new RealAdapterFactory(), scheduler,
                            new StartupChannelBootstrap.Jitter() {
                                public long delay(long capMillis) {
                                    return 1L;
                                }
                            }, new StartupChannelBootstrap.ClusterName() {
                                public String get(Application ignored) {
                                    return cluster + "_swarm";
                                }
                            }));
            Properties properties = new Properties();
            properties.setProperty("swarm.join.startupRetry", "true");
            properties.setProperty("swarm.join.minViewSize", "2");
            properties.setProperty("swarm.join.minViewWaitMillis", "500");
            properties.setProperty("swarm.join.retryInitialDelayMillis", "1000");
            properties.setProperty("swarm.join.retryMaxDelayMillis", "60000");

            setProxyEnabled(toxiproxy, false);
            run.bootstrap = bootstrap;
            run.state = state;
            run.policy = StartupJoinPolicy.parse(properties);
            bootstrapThread = new Thread(run, "jdbc-ping-bootstrap-it");
            bootstrapThread.start();

            require(channels.awaitSecondAttempt(20L, TimeUnit.SECONDS),
                    "bootstrap did not discard the blocked JDBC_PING attempt");
            Channel first = channels.firstChannel();
            require(first != null, "bootstrap did not create its first channel");
            require(scheduler.observedConnectedSingleton(),
                    "blocked JDBC_PING attempt was not observed as a connected singleton");
            require(!first.isOpen(),
                    "second attempt began before the singleton channel was closed");

            setProxyEnabled(toxiproxy, true);
            channels.releaseSecondAttempt();
            bootstrapThread.join(20000L);
            require(!bootstrapThread.isAlive(),
                    "bootstrap did not recover after JDBC_PING became reachable");
            if (run.failure != null) {
                throw new AssertionError("bootstrap failed", run.failure);
            }
            require(run.adapter != null, "bootstrap returned no adapter");
            Channel recovered = (Channel) run.adapter.getTransport();
            require(channels.createdCount() == 2,
                    "bootstrap did not recover with exactly one fresh retry");
            require(recovered != first, "bootstrap reused the failed channel");
            require(recovered.isConnected() && recovered.getView() != null
                            && recovered.getView().size() >= 2,
                    "fresh channel did not discover the healthy member");
            String result = "PASS JDBC_PING fresh-channel recovery viewSize="
                    + recovered.getView().size();
            writeReport(result + "\n");
            System.out.println(result);
        } finally {
            channels.releaseSecondAttempt();
            setProxyEnabled(toxiproxy, true);
            state.cancel();
            if (bootstrapThread != null) {
                bootstrapThread.join(5000L);
            }
            close(healthy);
        }
    }

    private static Channel connect(String url, String user, String password,
                                   int port, String cluster) throws Exception {
        JChannel channel = new JChannel(stack(url, user, password, port));
        channel.connect(cluster + "_swarm");
        return channel;
    }

    private static String stack(String url, String user, String password,
                                int port) {
        return "TCP(bind_addr=127.0.0.1;bind_port=" + port
                + ";port_range=0;loopback=true):"
                + "JDBC_PING(connection_driver=com.mysql.cj.jdbc.Driver;connection_url="
                + url + ";connection_username=" + user + ";connection_password="
                + password + ";initialize_sql=\"" + INITIALIZE_SQL + "\""
                + ";insert_single_sql=\"INSERT INTO JGROUPSPING "
                + "(own_addr, cluster_name, ping_data) values (?, ?, ?)\""
                + ";delete_single_sql=\"DELETE FROM JGROUPSPING "
                + "WHERE own_addr=? AND cluster_name=?\""
                + ";select_all_pingdata_sql=\"SELECT ping_data FROM JGROUPSPING "
                + "WHERE cluster_name=?\"):"
                + "MERGE2(min_interval=1000;max_interval=2000):"
                + "FD_SOCK:VERIFY_SUSPECT(timeout=500):"
                + "pbcast.NAKACK:UNICAST:pbcast.STABLE:"
                + "pbcast.GMS(join_timeout=2000;print_local_addr=false):"
                + "FRAG2:pbcast.STATE_TRANSFER";
    }

    private static void verifyDatabase(String url, String user,
                                       String password) throws Exception {
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

    private static void setProxyEnabled(String baseUrl, boolean enabled)
            throws IOException {
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

    private static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.length() == 0 ? fallback : value;
    }

    private static void writeReport(String report) throws IOException {
        File file = new File(System.getProperty("integration.report",
                "build/test-results/integration.txt"));
        File parent = file.getParentFile();
        if (parent != null && !parent.isDirectory() && !parent.mkdirs()) {
            throw new IOException("cannot create integration report directory");
        }
        FileOutputStream output = new FileOutputStream(file);
        try {
            output.write(report.getBytes("UTF-8"));
        } finally {
            output.close();
        }
    }

    private static void close(Channel channel) {
        if (channel != null) {
            channel.close();
        }
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }

    private static final class RealAdapterFactory
            implements StartupChannelBootstrap.AdapterFactory {
        public PullPushAdapter create(Channel channel) {
            return new PullPushAdapter(channel, null, null, false);
        }

        public void start(PullPushAdapter adapter) {
            adapter.start();
        }

        public void stop(PullPushAdapter adapter) {
            adapter.stop();
        }
    }

    private static final class BlockingChannelFactory
            implements StartupChannelBootstrap.ChannelFactory {
        private final String url;
        private final String user;
        private final String password;
        private final String cluster;
        private final CountDownLatch secondAttempt = new CountDownLatch(1);
        private final CountDownLatch releaseSecond = new CountDownLatch(1);
        private final List channels = new ArrayList();
        private int attempts;

        BlockingChannelFactory(String url, String user, String password,
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
                if (!releaseSecond.await(20L, TimeUnit.SECONDS)) {
                    throw new AssertionError("second bootstrap attempt was not released");
                }
            } else if (attempt > 2) {
                throw new AssertionError("unexpected bootstrap attempt " + attempt);
            }
            Channel channel = new JChannel(stack(
                    url, user, password, CANDIDATE_BIND_PORT));
            synchronized (this) {
                channels.add(channel);
            }
            return channel;
        }

        boolean awaitSecondAttempt(long timeout, TimeUnit unit)
                throws InterruptedException {
            return secondAttempt.await(timeout, unit);
        }

        void releaseSecondAttempt() {
            releaseSecond.countDown();
        }

        synchronized Channel firstChannel() {
            return channels.isEmpty() ? null : (Channel) channels.get(0);
        }

        synchronized Channel latestChannel() {
            return channels.isEmpty() ? null
                    : (Channel) channels.get(channels.size() - 1);
        }

        synchronized int createdCount() {
            return channels.size();
        }
    }

    private static final class ObservingScheduler
            implements StartupChannelBootstrap.Scheduler {
        private final BlockingChannelFactory channels;
        private volatile boolean connectedSingleton;

        ObservingScheduler(BlockingChannelFactory channels) {
            this.channels = channels;
        }

        public long nowMillis() {
            return System.currentTimeMillis();
        }

        public void sleep(long delayMillis) throws InterruptedException {
            Channel channel = channels.latestChannel();
            if (channel != null && channel.isConnected()
                    && channel.getView() != null && channel.getView().size() == 1) {
                connectedSingleton = true;
            }
            Thread.sleep(delayMillis);
        }

        boolean observedConnectedSingleton() {
            return connectedSingleton;
        }
    }

    private static final class BootstrapRun implements Runnable {
        StartupChannelBootstrap bootstrap;
        BootstrapState state;
        StartupJoinPolicy policy;
        volatile PullPushAdapter adapter;
        volatile Throwable failure;

        public void run() {
            try {
                adapter = bootstrap.bootstrap(
                        new Application("jdbc-ping-recovery-it"), state, policy);
            } catch (Throwable caught) {
                failure = caught;
            }
        }
    }
}
