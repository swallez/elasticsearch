import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.*;
import org.apache.arrow.vector.util.Text;
import java.io.*;
import java.util.List;

/** A few record batches with few, big rows: arguments: rowsPerBatch batches bytesPerString out.arrow */
public class GenBig {
    public static void main(String[] a) throws Exception {
        int rows = Integer.parseInt(a[0]), batches = Integer.parseInt(a[1]), size = Integer.parseInt(a[2]);
        String text = "lorem ipsum dolor sit amet ".repeat(size / 27 + 1);
        Schema schema = new Schema(List.of(new Field("ints", FieldType.nullable(new ArrowType.Int(32, true)), null), new Field("strings", FieldType.nullable(new ArrowType.Utf8()), null)));
        try (var alloc = new RootAllocator(); var root = VectorSchemaRoot.create(schema, alloc);
             var out = new BufferedOutputStream(new FileOutputStream(a[3])); var w = new ArrowStreamWriter(root, null, out)) {
            for (int b = 0; b < batches; b++) {
                for (int r = 0; r < rows; r++) { ((IntVector) root.getVector(0)).setSafe(r, b * rows + r); ((VarCharVector) root.getVector(1)).setSafe(r, new Text(text)); }
                root.setRowCount(rows); w.writeBatch();
            }
        }
    }
}
