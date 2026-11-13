package dev.cascade.testengine;

import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.state.api.OperatorIdentifier;
import org.apache.flink.state.api.SavepointWriter;
import org.apache.flink.state.api.runtime.SavepointLoader;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Savepoint inspection and surgery with Flink's State Processor API. One JSON document on stdout.
 *
 * <pre>
 * inspect SAVEPOINT [--uid UID]...
 *     the savepoint's operators: id, parallelism, max parallelism, state size, subtasks with state;
 *     each --uid (an operator UID as the compiler pins it) names the operators it matches
 * subset SAVEPOINT OUT [--drop X]... [--keep X]...
 *     write a new savepoint without the --drop operators, or with only the --keep operators.
 *     X is an operator UID, or a 32-character hex operator id (what inspect prints)
 * </pre>
 *
 * Operator state is the unit: a SQL operator's keyed state is serialised with types generated for
 * that operator, so it can't be filtered row by row without that operator's code. What can be
 * removed safely is a whole operator's state, which then starts empty when a job restores with
 * allow-non-restored-state off for it (it is simply absent from the savepoint).
 */
public final class StateTool {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Pattern HEX = Pattern.compile("[0-9a-fA-F]{32}");

    public static void main(String[] args) throws Exception {
        PrintStream out = new PrintStream(new java.io.FileOutputStream(java.io.FileDescriptor.out), true, "UTF-8");
        System.setOut(System.err); // keep logs off the JSON channel
        Map<String, Object> result = new LinkedHashMap<>();
        try {
            if (args.length < 2) {
                throw new IllegalArgumentException("usage: inspect SAVEPOINT [--uid UID]... | "
                        + "subset SAVEPOINT OUT [--drop X]... [--keep X]...");
            }
            switch (args[0]) {
                case "inspect":
                    result = inspect(args[1], flag(args, "--uid", 2));
                    break;
                case "subset":
                    if (args.length < 3) {
                        throw new IllegalArgumentException("subset needs SAVEPOINT and OUT");
                    }
                    result = subset(args[1], args[2], flag(args, "--drop", 3), flag(args, "--keep", 3));
                    break;
                default:
                    throw new IllegalArgumentException("unknown command " + args[0]);
            }
        } catch (Throwable t) {
            result.clear();
            StringBuilder why = new StringBuilder();
            for (Throwable c = t; c != null && why.length() < 1500; c = c.getCause() == c ? null : c.getCause()) {
                if (why.length() > 0) {
                    why.append(" <- ");
                }
                why.append(c.getMessage() == null ? c.toString() : c.getMessage().split("\n")[0]);
            }
            result.put("error", why.toString());
        }
        out.println(new String(JSON.writeValueAsBytes(result), StandardCharsets.UTF_8));
        out.flush();
        Runtime.getRuntime().halt(result.containsKey("error") ? 1 : 0);
    }

    private static List<String> flag(String[] args, String name, int from) {
        List<String> values = new ArrayList<>();
        for (int i = from; i < args.length - 1; i++) {
            if (args[i].equals(name)) {
                values.add(args[++i]);
            }
        }
        return values;
    }

    private static OperatorIdentifier identifier(String x) {
        return HEX.matcher(x).matches() ? OperatorIdentifier.forUidHash(x.toLowerCase()) : OperatorIdentifier.forUid(x);
    }

    private static String hex(OperatorIdentifier id) {
        return id.getOperatorId().toHexString();
    }

    private static Map<String, Object> inspect(String path, List<String> uids) throws Exception {
        CheckpointMetadata meta = SavepointLoader.loadSavepointMetadata(path);
        Map<String, String> known = new LinkedHashMap<>();
        for (String uid : uids) {
            known.put(hex(OperatorIdentifier.forUid(uid)), uid);
        }
        List<Object> operators = new ArrayList<>();
        long total = 0;
        Set<String> matched = new LinkedHashSet<>();
        for (OperatorState s : meta.getOperatorStates()) {
            String id = s.getOperatorID().toHexString();
            Map<String, Object> o = new LinkedHashMap<>();
            o.put("id", id);
            if (known.containsKey(id)) {
                o.put("uid", known.get(id));
                matched.add(known.get(id));
            }
            o.put("parallelism", s.getParallelism());
            o.put("max_parallelism", s.getMaxParallelism());
            o.put("subtasks_with_state", s.getStates().size());
            o.put("state_bytes", s.getCheckpointedSize());
            total += s.getCheckpointedSize();
            operators.add(o);
        }
        List<String> unmatched = new ArrayList<>();
        for (String uid : uids) {
            if (!matched.contains(uid)) {
                unmatched.add(uid);
            }
        }
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("savepoint", path);
        r.put("checkpoint_id", meta.getCheckpointId());
        r.put("operators", operators);
        r.put("state_bytes", total);
        if (!uids.isEmpty()) {
            r.put("uids_not_in_savepoint", unmatched);
        }
        return r;
    }

    private static Map<String, Object> subset(String in, String out, List<String> drop, List<String> keep)
            throws Exception {
        if (drop.isEmpty() == keep.isEmpty()) {
            throw new IllegalArgumentException("give --drop (remove these operators) or --keep (remove all others), "
                    + "one of them");
        }
        if (new java.io.File(out).exists()) {
            throw new IllegalArgumentException("output already exists: " + out);
        }
        CheckpointMetadata meta = SavepointLoader.loadSavepointMetadata(in);
        Set<String> present = new LinkedHashSet<>();
        for (OperatorState s : meta.getOperatorStates()) {
            present.add(s.getOperatorID().toHexString());
        }
        Set<String> remove = new LinkedHashSet<>();
        List<String> missing = new ArrayList<>();
        if (!drop.isEmpty()) {
            for (String x : drop) {
                String id = hex(identifier(x));
                if (present.contains(id)) {
                    remove.add(id);
                } else {
                    missing.add(x);
                }
            }
        } else {
            Set<String> keepIds = new LinkedHashSet<>();
            for (String x : keep) {
                String id = hex(identifier(x));
                keepIds.add(id);
                if (!present.contains(id)) {
                    missing.add(x);
                }
            }
            for (String id : present) {
                if (!keepIds.contains(id)) {
                    remove.add(id);
                }
            }
        }
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SavepointWriter writer = SavepointWriter.fromExistingSavepoint(env, in);
        for (String id : remove) {
            writer = writer.removeOperator(OperatorIdentifier.forUidHash(id));
        }
        writer.write(out);
        env.execute("cascade-state-subset");
        CheckpointMetadata written = SavepointLoader.loadSavepointMetadata(out);
        List<String> kept = new ArrayList<>();
        for (OperatorState s : written.getOperatorStates()) {
            kept.add(s.getOperatorID().toHexString());
        }
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("input", in);
        r.put("output", out);
        r.put("removed", new ArrayList<>(remove));
        r.put("kept", kept);
        r.put("not_in_savepoint", missing);
        return r;
    }
}
