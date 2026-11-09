import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.*;
import org.apache.arrow.vector.util.Text;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Random;

/** Writes the same rows as an Arrow IPC stream and as NDJSON bulk, for comparing ingestion paths. */
public class Gen {
    static Field f(String n, ArrowType t) { return new Field(n, FieldType.nullable(t), null); }

    public static void main(String[] a) throws Exception {
        int rowsPerBatch = Integer.parseInt(a[0]);
        int batches = Integer.parseInt(a[1]);
        String prefix = a[2];
        boolean withIds = a.length > 3 && a[3].equals("ids");
        Random r = new Random(42);
        String[] levels = { "debug", "info", "warn", "error", "fatal" };
        int[] statuses = { 200, 201, 204, 301, 400, 404, 500 };

        java.util.List<Field> fields = new java.util.ArrayList<>(List.of(
            f("@timestamp", new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)),
            f("host", new ArrowType.Utf8()), f("level", new ArrowType.Utf8()), f("service", new ArrowType.Utf8()),
            f("region", new ArrowType.Utf8()), f("message", new ArrowType.Utf8()),
            f("status", new ArrowType.Int(32, true)), f("bytes", new ArrowType.Int(64, true)),
            f("duration_ms", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
            f("user_id", new ArrowType.Int(64, true)), f("success", ArrowType.Bool.INSTANCE),
            f("bucket", new ArrowType.Int(32, true))));
        if (withIds) fields.add(f("_id", new ArrowType.Utf8()));
        Schema schema = new Schema(fields);

        try (var alloc = new RootAllocator();
             var root = VectorSchemaRoot.create(schema, alloc);
             var arrowOut = new BufferedOutputStream(new FileOutputStream(prefix + ".arrow"));
             var json = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(prefix + ".ndjson"), StandardCharsets.UTF_8));
             var writer = new ArrowStreamWriter(root, null, arrowOut)) {
            long ts = 1_700_000_000_000L;
            for (int b = 0; b < batches; b++) {
                for (int i = 0; i < rowsPerBatch; i++) {
                    String host = "host-" + r.nextInt(50);
                    String level = levels[r.nextInt(levels.length)];
                    String service = "svc-" + r.nextInt(20);
                    String region = r.nextInt(10) == 0 ? null : "region-" + r.nextInt(8);
                    int status = statuses[r.nextInt(statuses.length)];
                    long bytes = r.nextInt(1_000_000);
                    double dur = r.nextDouble() * 1000;
                    Long user = r.nextInt(10) == 0 ? null : (long) r.nextInt(10_000_000);
                    boolean ok = status < 400;
                    int bucket = r.nextInt(20);
                    String msg = "GET /api/v1/items/" + r.nextInt(1_000_000) + " completed in " + (int) dur + "ms for request " + Long.toHexString(r.nextLong());
                    long t = ts++;

                    ((TimeStampMilliVector) root.getVector(0)).setSafe(i, t);
                    ((VarCharVector) root.getVector(1)).setSafe(i, new Text(host));
                    ((VarCharVector) root.getVector(2)).setSafe(i, new Text(level));
                    ((VarCharVector) root.getVector(3)).setSafe(i, new Text(service));
                    if (region == null) ((VarCharVector) root.getVector(4)).setNull(i); else ((VarCharVector) root.getVector(4)).setSafe(i, new Text(region));
                    ((VarCharVector) root.getVector(5)).setSafe(i, new Text(msg));
                    ((IntVector) root.getVector(6)).setSafe(i, status);
                    ((BigIntVector) root.getVector(7)).setSafe(i, bytes);
                    ((Float8Vector) root.getVector(8)).setSafe(i, dur);
                    if (user == null) ((BigIntVector) root.getVector(9)).setNull(i); else ((BigIntVector) root.getVector(9)).setSafe(i, user);
                    ((BitVector) root.getVector(10)).setSafe(i, ok ? 1 : 0);
                    ((IntVector) root.getVector(11)).setSafe(i, bucket);
                    if (withIds) ((VarCharVector) root.getVector(12)).setSafe(i, new Text("doc-" + (b * rowsPerBatch + i)));

                    json.write("{\"index\":{}}\n");
                    json.write("{\"@timestamp\":" + t + ",\"host\":\"" + host + "\",\"level\":\"" + level + "\",\"service\":\"" + service + "\""
                        + (region == null ? "" : ",\"region\":\"" + region + "\"") + ",\"message\":\"" + msg + "\",\"status\":" + status
                        + ",\"bytes\":" + bytes + ",\"duration_ms\":" + dur + (user == null ? "" : ",\"user_id\":" + user)
                        + ",\"success\":" + ok + ",\"bucket\":" + bucket + "}\n");
                }
                root.setRowCount(rowsPerBatch);
                writer.writeBatch();
            }
        }
    }
}
