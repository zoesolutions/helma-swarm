package helma.swarm;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public final class AllTests {

    interface CheckedRunnable {
        void run() throws Exception;
    }

    private static final class TestCase {
        final String name;
        final boolean reproduction;
        final CheckedRunnable body;

        TestCase(String name, boolean reproduction, CheckedRunnable body) {
            this.name = name;
            this.reproduction = reproduction;
            this.body = body;
        }
    }

    static final class Suite {
        private final List<TestCase> tests = new ArrayList<TestCase>();

        void baseline(String name, CheckedRunnable body) {
            tests.add(new TestCase(name, false, body));
        }

        void reproduction(String name, CheckedRunnable body) {
            tests.add(new TestCase(name, true, body));
        }
    }

    private AllTests() {
    }

    public static void main(String[] args) throws Exception {
        String mode = args.length == 0 ? "all" : args[0];
        if (!"all".equals(mode) && !"baseline".equals(mode) && !"red".equals(mode)) {
            System.err.println("Usage: helma.swarm.AllTests [baseline|red|all]");
            System.exit(2);
        }

        Suite suite = new Suite();
        LegacyBehaviorTest.register(suite);
        StartupChannelBootstrapTest.register(suite);
        InitialSessionStateTest.register(suite);

        List<TestCase> selected = new ArrayList<TestCase>();
        for (TestCase test : suite.tests) {
            if ("all".equals(mode)
                    || ("baseline".equals(mode) && !test.reproduction)
                    || ("red".equals(mode) && test.reproduction)) {
                selected.add(test);
            }
        }

        List<String> failures = new ArrayList<String>();
        long started = System.currentTimeMillis();
        for (TestCase test : selected) {
            try {
                test.body.run();
                System.out.println("PASS [" + label(test) + "] " + test.name);
            } catch (Throwable failure) {
                String message = failure.getClass().getSimpleName() + ": " + safeMessage(failure);
                failures.add(test.name + "\t" + message);
                System.out.println("FAIL [" + label(test) + "] " + test.name + " - " + message);
            }
        }

        long elapsed = System.currentTimeMillis() - started;
        writeXmlReport(selected, failures, elapsed);
        System.out.println("RESULT mode=" + mode + " tests=" + selected.size()
                + " failures=" + failures.size() + " elapsedMillis=" + elapsed);
        if (!failures.isEmpty()) {
            System.exit(1);
        }
    }

    static void assertTrue(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }

    static void assertEquals(Object expected, Object actual, String message) {
        if (expected == null ? actual != null : !expected.equals(actual)) {
            throw new AssertionError(message + " expected=<" + expected
                    + "> actual=<" + actual + ">");
        }
    }

    private static String label(TestCase test) {
        return test.reproduction ? "RED REPRO" : "BASELINE";
    }

    private static String safeMessage(Throwable failure) {
        return failure.getMessage() == null ? "(no message)" : failure.getMessage();
    }

    private static void writeXmlReport(List<TestCase> tests, List<String> failures,
                                       long elapsed) throws IOException {
        String reportPath = System.getProperty("test.report", "build/test-results/unit.xml");
        File report = new File(reportPath);
        File parent = report.getParentFile();
        if (parent != null && !parent.isDirectory() && !parent.mkdirs()) {
            throw new IOException("Cannot create report directory: " + parent);
        }

        StringBuilder xml = new StringBuilder();
        xml.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
        xml.append("<testsuite name=\"helma.swarm.AllTests\" tests=\"")
                .append(tests.size()).append("\" failures=\"").append(failures.size())
                .append("\" time=\"").append(elapsed / 1000.0d).append("\">\n");
        for (TestCase test : tests) {
            xml.append("  <testcase classname=\"helma.swarm\" name=\"")
                    .append(xml(test.name)).append("\">");
            String failure = findFailure(test.name, failures);
            if (failure != null) {
                xml.append("<failure message=\"").append(xml(failure)).append("\"/>");
            }
            xml.append("</testcase>\n");
        }
        xml.append("</testsuite>\n");
        FileOutputStream output = new FileOutputStream(report);
        try {
            output.write(xml.toString().getBytes(StandardCharsets.UTF_8));
        } finally {
            output.close();
        }
    }

    private static String findFailure(String name, List<String> failures) {
        String prefix = name + "\t";
        for (String failure : failures) {
            if (failure.startsWith(prefix)) {
                return failure.substring(prefix.length());
            }
        }
        return null;
    }

    private static String xml(String value) {
        return value.replace("&", "&amp;").replace("<", "&lt;")
                .replace(">", "&gt;").replace("\"", "&quot;");
    }
}
