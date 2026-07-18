package helma.swarm;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public final class BootstrapSigtermTest {

    private static final String[] MODES = new String[] {
        "view-wait", "backoff", "invalid-config", "invalid-config-waiters",
        "adapter-start", "published-cleanup", "session-wait"
    };

    private BootstrapSigtermTest() {
    }

    public static void main(String[] args) throws Exception {
        StringBuffer report = new StringBuffer();
        for (int i = 0; i < MODES.length; i++) {
            report.append(verifyMode(MODES[i])).append('\n');
        }
        String result = "SIGTERM_RESULT modes=" + MODES.length + " failures=0";
        report.append(result).append('\n');
        writeReport(report.toString());
        System.out.println(result);
    }

    private static String verifyMode(final String mode) throws Exception {
        String java = System.getProperty("java.home") + "/bin/java";
        ProcessBuilder builder = new ProcessBuilder(java, "-cp",
                System.getProperty("java.class.path"),
                BootstrapSigtermMain.class.getName(), mode);
        builder.redirectErrorStream(true);
        final Process process = builder.start();
        final StringBuffer output = new StringBuffer();
        final CountDownLatch ready = new CountDownLatch(1);
        final AtomicReference readFailure = new AtomicReference();
        Thread reader = new Thread(new Runnable() {
            public void run() {
                try {
                    BufferedReader lines = new BufferedReader(
                            new InputStreamReader(process.getInputStream(), "UTF-8"));
                    String line;
                    while ((line = lines.readLine()) != null) {
                        synchronized (output) {
                            output.append(line).append('\n');
                        }
                        if (("SIGTERM_READY " + mode).equals(line)) {
                            ready.countDown();
                        }
                    }
                } catch (Throwable failure) {
                    readFailure.set(failure);
                }
            }
        }, "sigterm-output-" + mode);
        reader.setDaemon(true);
        reader.start();

        if (!ready.await(5L, TimeUnit.SECONDS)) {
            process.destroyForcibly();
            throw new AssertionError("process did not reach " + mode + ": " + output);
        }
        long started = System.currentTimeMillis();
        int pid = processId(process);
        Process signal = new ProcessBuilder("/bin/kill", "-TERM",
                String.valueOf(pid)).start();
        if (signal.waitFor() != 0) {
            process.destroyForcibly();
            throw new AssertionError("could not send SIGTERM to " + mode);
        }
        if (!process.waitFor(10L, TimeUnit.SECONDS)) {
            process.destroyForcibly();
            throw new AssertionError("SIGTERM did not stop " + mode + ": " + output);
        }
        long elapsed = System.currentTimeMillis() - started;
        reader.join(1000L);
        if (readFailure.get() != null) {
            throw new AssertionError("could not read " + mode + ": "
                    + readFailure.get());
        }
        if (process.exitValue() != 143) {
            throw new AssertionError("unexpected SIGTERM exit for " + mode
                    + ": " + process.exitValue());
        }
        if (elapsed >= 5000L) {
            throw new AssertionError("slow SIGTERM for " + mode + ": " + elapsed);
        }
        if ("published-cleanup".equals(mode)
                && output.indexOf("startup cleanup deadline exceeded") < 0) {
            throw new AssertionError("blocked cleanup deadline was not observed: "
                    + output);
        }
        String result = "SIGTERM_PASS mode=" + mode + " exit="
                + process.exitValue() + " elapsedMillis=" + elapsed;
        System.out.println(result);
        return result;
    }

    private static int processId(Process process) throws Exception {
        Field field = process.getClass().getDeclaredField("pid");
        field.setAccessible(true);
        return field.getInt(process);
    }

    private static void writeReport(String report) throws Exception {
        File file = new File(System.getProperty("process.report",
                "build/test-results/process.txt"));
        File parent = file.getParentFile();
        if (parent != null && !parent.isDirectory() && !parent.mkdirs()) {
            throw new IllegalStateException("cannot create process report directory");
        }
        FileOutputStream output = new FileOutputStream(file);
        try {
            output.write(report.getBytes("UTF-8"));
        } finally {
            output.close();
        }
    }
}
