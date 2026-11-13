package dev.cascade.testengine;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.table.types.utils.TypeConversions;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The cascade-test engine: a long-lived JVM that runs pipeline tests on one shared MiniCluster.
 *
 * <p>Protocol: one JSON request per line on stdin; each response is one line on stdout prefixed
 * with {@code @@cascade-test }. Everything else the JVM prints goes to stderr.
 *
 * <pre>
 * {"cmd": "run", "tests": [test, ...]}      -> {"results": [result, ...]}   (same order)
 * {"cmd": "describe", "setup": [sql, ...]}  -> {"views": {name: [column, ...]}}
 * {"cmd": "ping"}                           -> {"pong": true}
 * {"cmd": "shutdown"}                       -> {"bye": true}
 *
 * test   = {"id", "setup": [sql], "outputs": [output], "timeout": seconds, "config": {k: v}}
 * output = {"name", "query": "SELECT ..."}                  capture a query's full changelog
 *        | {"name", "insert": "INSERT INTO t ...", "table": t, "capture": id}
 *                                                           run a real INSERT into a sink declared
 *                                                           in setup with connector cascade-capture
 * result = {"id", "millis", "outputs": {name: {"columns", "rows": [{"kind", "values"}]}},
 *           "error"?, "phase"? (plan|run|timeout), "expectation_violation"?}
 * </pre>
 *
 * All outputs of a test run as one job (a statement set), so a whole-pipeline test sees every sink
 * fed from the same execution, just like production.
 */
public final class Engine {

    private static final String PREFIX = "@@cascade-test ";
    private static final Pattern SET = Pattern.compile("(?s)^SET\\s+'(.*?)'\\s*=\\s*'(.*)'\\s*;?$");
    private static final ObjectMapper JSON = new ObjectMapper();

    private static PrintStream protocol;
    private static ExecutorService workers;
    private static ExecutorService waiters;

    public static void main(String[] args) throws Exception {
        protocol = new PrintStream(new java.io.FileOutputStream(java.io.FileDescriptor.out), true, "UTF-8");
        System.setOut(System.err); // keep stray output (logs, user prints) off the protocol channel
        int slots = Integer.parseInt(System.getProperty("cascade.slots", "8"));
        int parallelTests = Integer.parseInt(System.getProperty("cascade.workers", "4"));
        workers = Executors.newFixedThreadPool(parallelTests);
        waiters = Executors.newCachedThreadPool();
        long t0 = System.currentTimeMillis();
        SharedCluster.start(slots);
        Map<String, Object> ready = new LinkedHashMap<>();
        ready.put("ready", true);
        ready.put("flink_version", EnvironmentInformation.getVersion());
        ready.put("startup_millis", System.currentTimeMillis() - t0);
        ready.put("slots", slots);
        ready.put("workers", parallelTests);
        respond(ready);

        BufferedReader in = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
        String line;
        while ((line = in.readLine()) != null) {
            if (line.trim().isEmpty()) {
                continue;
            }
            Map<String, Object> req;
            try {
                req = JSON.readValue(line, Map.class);
            } catch (Exception e) {
                respond(error("bad request: " + e.getMessage()));
                continue;
            }
            String cmd = String.valueOf(req.get("cmd"));
            try {
                switch (cmd) {
                    case "run":
                        respond(run(list(req.get("tests"))));
                        break;
                    case "describe":
                        respond(describe(strings(req.get("setup")), map(req.get("config"))));
                        break;
                    case "ping":
                        respond(single("pong", true));
                        break;
                    case "shutdown":
                        respond(single("bye", true));
                        shutdown();
                        return;
                    default:
                        respond(error("unknown command " + cmd));
                }
            } catch (Throwable t) {
                respond(error(errorMessage(t)));
            }
        }
        shutdown();
    }

    private static void shutdown() {
        workers.shutdownNow();
        waiters.shutdownNow();
        SharedCluster.stop();
        Runtime.getRuntime().halt(0); // Flink leaves non-daemon threads behind
    }

    private static synchronized void respond(Map<String, Object> out) throws Exception {
        protocol.println(PREFIX + JSON.writeValueAsString(out));
        protocol.flush();
    }

    // --- run ------------------------------------------------------------------------------

    private static Map<String, Object> run(List<Object> tests) throws Exception {
        List<Future<Map<String, Object>>> futures = new ArrayList<>();
        for (Object t : tests) {
            Map<String, Object> test = map(t);
            futures.add(workers.submit(() -> runTest(test)));
        }
        List<Object> results = new ArrayList<>();
        for (Future<Map<String, Object>> f : futures) {
            results.add(f.get());
        }
        return single("results", results);
    }

    static TableEnvironment newEnv(Map<String, Object> config) {
        Configuration conf = new Configuration();
        conf.setString("parallelism.default", "1");
        conf.setString("restart-strategy.type", "none");
        conf.setString("table.local-time-zone", "UTC");
        conf.setString("table.dml-sync", "false");
        String python = System.getenv("CASCADE_PYTHON");
        if (python != null && !python.isEmpty()) {
            conf.setString("python.executable", python);
            conf.setString("python.client.executable", python);
        }
        if (config != null) {
            for (Map.Entry<String, Object> e : config.entrySet()) {
                conf.setString(e.getKey(), String.valueOf(e.getValue()));
            }
        }
        return TableEnvironment.create(
                EnvironmentSettings.newInstance().inStreamingMode().withConfiguration(conf).build());
    }

    static void setup(TableEnvironment tEnv, List<String> statements) {
        for (String raw : statements) {
            String s = raw.trim();
            if (s.endsWith(";")) {
                s = s.substring(0, s.length() - 1);
            }
            Matcher m = SET.matcher(s);
            if (m.matches()) {
                tEnv.getConfig().getConfiguration().setString(m.group(1), m.group(2).replace("''", "'"));
            } else if (s.startsWith("-- @@")) {
                // cascade runner directives (replication-slot guards): they act on restore, and a test
                // never restores from a live database, so there is nothing to check here
                continue;
            } else if (!s.isEmpty()) {
                tEnv.executeSql(s);
            }
        }
    }

    private static Map<String, Object> runTest(Map<String, Object> test) {
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("id", test.get("id"));
        long start = System.currentTimeMillis();
        int timeout = test.get("timeout") == null ? 60 : ((Number) test.get("timeout")).intValue();
        String phase = "plan";
        JobClient job = null;
        Map<String, String> captures = new LinkedHashMap<>();
        Map<String, Object> outputs = new LinkedHashMap<>();
        try {
            TableEnvironment tEnv = newEnv(map(test.get("config")));
            setup(tEnv, strings(test.get("setup")));
            StatementSet set = tEnv.createStatementSet();
            for (Object o : list(test.get("outputs"))) {
                Map<String, Object> out = map(o);
                String name = String.valueOf(out.get("name"));
                Map<String, Object> entry = new LinkedHashMap<>();
                if (out.get("insert") != null) {
                    String table = String.valueOf(out.get("table"));
                    entry.put("columns", columns(tEnv.from("`" + table + "`").getResolvedSchema()));
                    set.addInsertSql(stripSemicolon(String.valueOf(out.get("insert"))));
                    captures.put(name, String.valueOf(out.get("capture")));
                } else {
                    Table t = tEnv.sqlQuery(stripSemicolon(String.valueOf(out.get("query"))));
                    ResolvedSchema schema = t.getResolvedSchema();
                    entry.put("columns", columns(schema));
                    String id = "q-" + UUID.randomUUID();
                    set.add(t.insertInto(TableDescriptor.forConnector("cascade-capture")
                            .schema(physical(schema))
                            .option("id", id)
                            .option("mode", "all")
                            .build()));
                    captures.put(name, id);
                }
                outputs.put(name, entry);
            }
            TableResult result = set.execute();
            phase = "run";
            job = result.getJobClient().orElseThrow(() -> new IllegalStateException("no job client"));
            JobClient running = job;
            Future<?> done = waiters.submit(() -> running.getJobExecutionResult().get());
            try {
                done.get(timeout, TimeUnit.SECONDS);
            } catch (TimeoutException e) {
                phase = "timeout";
                throw new TimeoutException("timed out after " + timeout + "s (is a source unbounded?)");
            }
            job = null;
            for (Map.Entry<String, String> c : captures.entrySet()) {
                @SuppressWarnings("unchecked")
                Map<String, Object> entry = (Map<String, Object>) outputs.get(c.getKey());
                entry.put("rows", CaptureSinkFactory.take(c.getValue()));
            }
            r.put("outputs", outputs);
        } catch (Throwable t) {
            r.put("error", errorMessage(t));
            r.put("phase", phase);
            r.put("expectation_violation", hasCause(t, "ExpectationViolation"));
            // partial output helps explain a failure
            for (Map.Entry<String, String> c : captures.entrySet()) {
                @SuppressWarnings("unchecked")
                Map<String, Object> entry = (Map<String, Object>) outputs.get(c.getKey());
                if (entry != null) {
                    entry.put("rows", CaptureSinkFactory.take(c.getValue()));
                }
            }
            r.put("outputs", outputs);
        } finally {
            if (job != null) {
                try {
                    job.cancel().get(10, TimeUnit.SECONDS);
                } catch (Throwable ignored) {
                    // finished or already gone
                }
            }
            for (String id : captures.values()) {
                CaptureSinkFactory.STORE.remove(id);
            }
        }
        r.put("millis", System.currentTimeMillis() - start);
        return r;
    }

    /** The schema of a query as a plain table schema (time attributes become ordinary timestamps). */
    static Schema physical(ResolvedSchema schema) {
        Schema.Builder b = Schema.newBuilder();
        for (Column c : schema.getColumns()) {
            b.column(c.getName(), plainType(c.getDataType()));
        }
        return b.build();
    }

    static DataType plainType(DataType t) {
        LogicalType lt = t.getLogicalType();
        if (LogicalTypeChecks.isTimeAttribute(lt)) {
            if (lt instanceof TimestampType) {
                lt = new TimestampType(lt.isNullable(), TimestampKind.REGULAR, ((TimestampType) lt).getPrecision());
            } else if (lt instanceof LocalZonedTimestampType) {
                lt = new LocalZonedTimestampType(
                        lt.isNullable(), TimestampKind.REGULAR, ((LocalZonedTimestampType) lt).getPrecision());
            }
            return TypeConversions.fromLogicalToDataType(lt);
        }
        return t;
    }

    static List<Object> columns(ResolvedSchema schema) {
        List<Object> cols = new ArrayList<>();
        for (Column c : schema.getColumns()) {
            LogicalType t = c.getDataType().getLogicalType();
            Map<String, Object> col = new LinkedHashMap<>();
            col.put("name", c.getName());
            String timeAttr = null;
            if (LogicalTypeChecks.isRowtimeAttribute(t)) {
                timeAttr = "rowtime";
            } else if (LogicalTypeChecks.isProctimeAttribute(t)) {
                timeAttr = "proctime";
            }
            col.put("type", t.asSummaryString().replace(" *ROWTIME*", "").replace(" *PROCTIME*", ""));
            col.put("time_attribute", timeAttr);
            cols.add(col);
        }
        return cols;
    }

    // --- describe ---------------------------------------------------------------------------

    private static Map<String, Object> describe(List<String> setup, Map<String, Object> config) {
        Map<String, Object> out = new LinkedHashMap<>();
        Map<String, Object> views = new LinkedHashMap<>();
        try {
            TableEnvironment tEnv = newEnv(config);
            setup(tEnv, setup);
            for (String view : tEnv.listTemporaryViews()) {
                try {
                    views.put(view, columns(tEnv.from("`" + view + "`").getResolvedSchema()));
                } catch (Throwable t) {
                    views.put(view, single("error", errorMessage(t)));
                }
            }
        } catch (Throwable t) {
            out.put("error", errorMessage(t));
        }
        out.put("views", views);
        return out;
    }

    // --- helpers ----------------------------------------------------------------------------

    private static String stripSemicolon(String s) {
        s = s.trim();
        return s.endsWith(";") ? s.substring(0, s.length() - 1) : s;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> map(Object o) {
        return o == null ? new LinkedHashMap<>() : (Map<String, Object>) o;
    }

    @SuppressWarnings("unchecked")
    private static List<Object> list(Object o) {
        return o == null ? new ArrayList<>() : (List<Object>) o;
    }

    private static List<String> strings(Object o) {
        List<String> out = new ArrayList<>();
        for (Object x : list(o)) {
            out.add(String.valueOf(x));
        }
        return out;
    }

    private static Map<String, Object> single(String k, Object v) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put(k, v);
        return m;
    }

    private static Map<String, Object> error(String message) {
        return single("error", message);
    }

    static String errorMessage(Throwable t) {
        List<String> parts = new ArrayList<>();
        for (Throwable c = t; c != null && parts.size() < 12; c = c.getCause()) {
            String m = c.getClass().getSimpleName() + ": " + c.getMessage();
            if (parts.isEmpty() || !parts.get(parts.size() - 1).equals(m)) {
                parts.add(m);
            }
        }
        return String.join("\n  caused by ", parts);
    }

    private static boolean hasCause(Throwable t, String simpleName) {
        for (Throwable c = t; c != null; c = c.getCause()) {
            if (c.getClass().getSimpleName().equals(simpleName)) {
                return true;
            }
        }
        return false;
    }
}
