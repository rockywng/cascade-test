package dev.cascade.testengine;

import org.apache.flink.types.Row;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Flink external values -> JSON-friendly values (the Python side normalizes by column type). */
final class Values {

    private static final DateTimeFormatter TS = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    private Values() {}

    static Object encode(Object v) {
        if (v == null || v instanceof Boolean || v instanceof Integer || v instanceof Long
                || v instanceof Short || v instanceof Byte || v instanceof String) {
            return v;
        }
        if (v instanceof Double || v instanceof Float) {
            double d = ((Number) v).doubleValue();
            return Double.isFinite(d) ? v : String.valueOf(d);
        }
        if (v instanceof BigDecimal) {
            return ((BigDecimal) v).toPlainString();
        }
        if (v instanceof LocalDateTime) {
            return TS.format((LocalDateTime) v);
        }
        if (v instanceof java.time.Instant) {
            return TS.format(LocalDateTime.ofInstant((java.time.Instant) v, java.time.ZoneOffset.UTC)) + "Z";
        }
        if (v instanceof byte[]) {
            return Base64.getEncoder().encodeToString((byte[]) v);
        }
        if (v instanceof Row) {
            Row row = (Row) v;
            List<Object> l = new ArrayList<>();
            for (int i = 0; i < row.getArity(); i++) {
                l.add(encode(row.getField(i)));
            }
            return l;
        }
        if (v instanceof Object[]) {
            List<Object> l = new ArrayList<>();
            for (Object o : (Object[]) v) {
                l.add(encode(o));
            }
            return l;
        }
        if (v instanceof Map) {
            Map<String, Object> m = new LinkedHashMap<>();
            for (Map.Entry<?, ?> e : ((Map<?, ?>) v).entrySet()) {
                m.put(String.valueOf(e.getKey()), encode(e.getValue()));
            }
            return m;
        }
        return v.toString(); // LocalDate, LocalTime, ...
    }
}
