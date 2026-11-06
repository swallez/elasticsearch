/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.arrow.bulk;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryEncoder;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.elasticsearch.arrow.Arrow;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.escf.EscfBatch;
import org.elasticsearch.sourcebatch.SourceRowToXContent;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.BytesRefRecycler;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.BitSet;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class ArrowToEscfTests extends ESTestCase {

    private BufferAllocator allocator;
    private final ArrowToEscf converter = new ArrowToEscf(BytesRefRecycler.NON_RECYCLING_INSTANCE);

    @Before
    public void createAllocator() {
        allocator = Arrow.newChildAllocator("test", 0, Long.MAX_VALUE);
    }

    @After
    public void closeAllocator() {
        allocator.close();
    }

    private static Field field(String name, ArrowType type) {
        return new Field(name, FieldType.nullable(type), null);
    }

    private static BitSet allColumns(VectorSchemaRoot root) {
        BitSet columns = new BitSet();
        columns.set(0, root.getFieldVectors().size());
        return columns;
    }

    private static Map<String, Object> row(EscfBatch batch, int row) throws IOException {
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            SourceRowToXContent.writeRow(batch.row(row), batch.schema(), builder);
            return XContentHelper.convertToMap(BytesReference.bytes(builder), false, XContentType.JSON).v2();
        }
    }

    public void testScalars() throws IOException {
        var schema = new Schema(
            List.of(
                field("int", new ArrowType.Int(32, true)),
                field("long", new ArrowType.Int(64, true)),
                field("uint", new ArrowType.Int(32, false)),
                field("double", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
                field("bool", new ArrowType.Bool()),
                field("string", new ArrowType.Utf8()),
                field("ts", new ArrowType.Timestamp(TimeUnit.MILLISECOND, null))
            )
        );

        try (var root = VectorSchemaRoot.create(schema, allocator)) {
            ((IntVector) root.getVector("int")).setSafe(0, -42);
            ((BigIntVector) root.getVector("long")).setSafe(0, Long.MAX_VALUE);
            // Unsigned: must not become negative
            ((UInt4Vector) root.getVector("uint")).setSafe(0, 0xFFFF_FFFF);
            ((Float8Vector) root.getVector("double")).setSafe(0, 1.5);
            ((BitVector) root.getVector("bool")).setSafe(0, 1);
            ((VarCharVector) root.getVector("string")).setSafe(0, new Text("héllo"));
            ((TimeStampMilliVector) root.getVector("ts")).setSafe(0, 1_700_000_000_000L);

            // The second row has only nulls (absent values)
            for (var vector : root.getFieldVectors()) {
                vector.setNull(1);
                vector.setValueCount(2);
            }
            root.setRowCount(2);

            try (EscfBatch batch = converter.convert(root, Map.of(), allColumns(root), null)) {
                assertThat(batch.docCount(), equalTo(2));
                assertThat(
                    row(batch, 0),
                    equalTo(
                        Map.of(
                            "int",
                            -42,
                            "long",
                            Long.MAX_VALUE,
                            "uint",
                            4_294_967_295L,
                            "double",
                            1.5,
                            "bool",
                            true,
                            "string",
                            "héllo",
                            "ts",
                            1_700_000_000_000L
                        )
                    )
                );
                // Nulls are stored as absent values
                assertThat(row(batch, 1), equalTo(Map.of()));
            }
        }
    }

    public void testRowSubsetAndMetadataColumns() throws IOException {
        var schema = new Schema(List.of(field("_id", new ArrowType.Utf8()), field("n", new ArrowType.Int(32, true))));
        try (var root = VectorSchemaRoot.create(schema, allocator)) {
            for (int i = 0; i < 4; i++) {
                ((VarCharVector) root.getVector("_id")).setSafe(i, new Text("id" + i));
                ((IntVector) root.getVector("n")).setSafe(i, i);
            }
            root.setRowCount(4);

            assertThat(converter.stringValue(root.getVector("_id"), 2, Map.of()), equalTo("id2"));

            // Only column "n", and only rows 3 and 1, in that order. The metadata column isn't part of documents.
            BitSet values = new BitSet();
            values.set(1);
            try (EscfBatch batch = converter.convert(root, Map.of(), values, new int[] { 3, 1 })) {
                assertThat(batch.docCount(), equalTo(2));
                assertThat(row(batch, 0), equalTo(Map.of("n", 3)));
                assertThat(row(batch, 1), equalTo(Map.of("n", 1)));
            }
        }
    }

    public void testStructAndList() throws IOException {
        var structField = new Field(
            "obj",
            FieldType.nullable(ArrowType.Struct.INSTANCE),
            List.of(field("a", new ArrowType.Int(32, true)), field("b", new ArrowType.Utf8()))
        );
        var listField = new Field(
            "list",
            FieldType.nullable(ArrowType.List.INSTANCE),
            List.of(field("$data$", new ArrowType.Int(32, true)))
        );
        var stringListField = new Field(
            "strings",
            FieldType.nullable(ArrowType.List.INSTANCE),
            List.of(field("$data$", new ArrowType.Utf8()))
        );

        try (var root = VectorSchemaRoot.create(new Schema(List.of(structField, listField, stringListField)), allocator)) {
            var struct = (StructVector) root.getVector("obj");
            ((IntVector) struct.getChild("a")).setSafe(0, 7);
            ((VarCharVector) struct.getChild("b")).setSafe(0, new Text("x"));
            struct.setIndexDefined(0);
            struct.setValueCount(1);

            var list = (ListVector) root.getVector("list");
            list.startNewValue(0);
            var ints = (IntVector) list.getDataVector();
            ints.setSafe(0, 1);
            ints.setSafe(1, 2);
            ints.setSafe(2, 3);
            list.endValue(0, 3);
            list.setValueCount(1);

            var strings = (ListVector) root.getVector("strings");
            strings.startNewValue(0);
            var data = (VarCharVector) strings.getDataVector();
            data.setSafe(0, new Text("foo"));
            data.setSafe(1, new Text("bar"));
            strings.endValue(0, 2);
            strings.setValueCount(1);

            root.setRowCount(1);

            try (EscfBatch batch = converter.convert(root, Map.of(), allColumns(root), null)) {
                assertThat(
                    row(batch, 0),
                    equalTo(Map.of("obj", Map.of("a", 7, "b", "x"), "list", List.of(1, 2, 3), "strings", List.of("foo", "bar")))
                );
            }
        }
    }

    public void testDictionaryEncodedStrings() throws IOException {
        var dictField = new Field("dict", new FieldType(true, new ArrowType.Utf8(), new DictionaryEncoding(1L, false, null)), null);
        try (var dictVector = new VarCharVector("dictionary", allocator); var plain = new VarCharVector("dict", allocator)) {
            dictVector.setSafe(0, new Text("red"));
            dictVector.setSafe(1, new Text("green"));
            dictVector.setValueCount(2);
            var dictionary = new Dictionary(dictVector, new DictionaryEncoding(1L, false, null));

            plain.setSafe(0, new Text("green"));
            plain.setSafe(1, new Text("red"));
            plain.setNull(2);
            plain.setSafe(3, new Text("green"));
            plain.setValueCount(4);

            try (var encoded = (IntVector) DictionaryEncoder.encode(plain, dictionary); var root = new VectorSchemaRoot(List.of(encoded))) {
                root.setRowCount(4);
                assertThat(encoded.getField().getDictionary().getId(), equalTo(1L));
                try (EscfBatch batch = converter.convert(root, Map.of(1L, dictionary), allColumns(root), null)) {
                    assertThat(row(batch, 0), equalTo(Map.of("dict", "green")));
                    assertThat(row(batch, 1), equalTo(Map.of("dict", "red")));
                    assertThat(row(batch, 2), equalTo(Map.of()));
                    assertThat(row(batch, 3), equalTo(Map.of("dict", "green")));
                }
            }
        }
    }

    public void testNullStructHasNoChildren() throws IOException {
        var structField = new Field("obj", FieldType.nullable(ArrowType.Struct.INSTANCE), List.of(field("a", new ArrowType.Int(32, true))));
        try (var root = VectorSchemaRoot.create(new Schema(List.of(structField)), allocator)) {
            var struct = (StructVector) root.getVector("obj");
            var a = (IntVector) struct.getChild("a");
            // Row 0 is defined, row 1 is a null struct whose child holds a value that must be ignored
            a.setSafe(0, 1);
            struct.setIndexDefined(0);
            a.setSafe(1, 99);
            struct.setNull(1);
            a.setSafe(2, 3);
            struct.setIndexDefined(2);
            a.setValueCount(3);
            struct.setValueCount(3);
            root.setRowCount(3);

            try (EscfBatch batch = converter.convert(root, Map.of(), allColumns(root), null)) {
                assertThat(row(batch, 0), equalTo(Map.of("obj", Map.of("a", 1))));
                assertThat(row(batch, 1), equalTo(Map.of()));
                assertThat(row(batch, 2), equalTo(Map.of("obj", Map.of("a", 3))));
            }
        }
    }

    public void testBooleanListAndSparseLists() throws IOException {
        var boolList = new Field("bools", FieldType.nullable(ArrowType.List.INSTANCE), List.of(field("$data$", ArrowType.Bool.INSTANCE)));
        var doubleList = new Field(
            "doubles",
            FieldType.nullable(ArrowType.List.INSTANCE),
            List.of(field("$data$", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)))
        );
        try (var root = VectorSchemaRoot.create(new Schema(List.of(boolList, doubleList)), allocator)) {
            var bools = (ListVector) root.getVector("bools");
            // Row 0 has no value, row 1 has [true, false]
            bools.setNull(0);
            bools.startNewValue(1);
            ((BitVector) bools.getDataVector()).setSafe(0, 1);
            ((BitVector) bools.getDataVector()).setSafe(1, 0);
            bools.endValue(1, 2);
            bools.setValueCount(2);

            var doubles = (ListVector) root.getVector("doubles");
            // Row 0 has [0.5], row 1 has an empty list
            doubles.startNewValue(0);
            ((Float8Vector) doubles.getDataVector()).setSafe(0, 0.5);
            doubles.endValue(0, 1);
            doubles.startNewValue(1);
            doubles.endValue(1, 0);
            doubles.setValueCount(2);
            root.setRowCount(2);

            try (EscfBatch batch = converter.convert(root, Map.of(), allColumns(root), null)) {
                assertThat(row(batch, 0), equalTo(Map.of("doubles", List.of(0.5))));
                assertThat(row(batch, 1), equalTo(Map.of("bools", List.of(true, false), "doubles", List.of())));
            }
        }
    }

    public void testMapWithScalarValues() throws IOException {
        var entries = new Field(
            "entries",
            FieldType.notNullable(ArrowType.Struct.INSTANCE),
            List.of(new Field("key", FieldType.notNullable(new ArrowType.Utf8()), null), field("value", new ArrowType.Int(32, true)))
        );
        var mapField = new Field("map", FieldType.nullable(new ArrowType.Map(false)), List.of(entries));
        try (var root = VectorSchemaRoot.create(new Schema(List.of(mapField)), allocator)) {
            var map = (MapVector) root.getVector("map");
            var writer = map.getWriter();
            writer.setPosition(0);
            writer.startMap();
            writer.startEntry();
            writer.key().varChar().writeVarChar(new Text("a"));
            writer.value().integer().writeInt(1);
            writer.endEntry();
            writer.startEntry();
            writer.key().varChar().writeVarChar(new Text("b"));
            writer.value().integer().writeInt(2);
            writer.endEntry();
            writer.endMap();
            writer.setPosition(1);
            writer.startMap();
            writer.startEntry();
            writer.key().varChar().writeVarChar(new Text("b"));
            writer.value().integer().writeInt(3);
            writer.endEntry();
            writer.endMap();
            map.setValueCount(2);
            root.setRowCount(2);

            try (EscfBatch batch = converter.convert(root, Map.of(), allColumns(root), null)) {
                assertThat(row(batch, 0), equalTo(Map.of("map", Map.of("a", 1, "b", 2))));
                assertThat(row(batch, 1), equalTo(Map.of("map", Map.of("b", 3))));
            }
        }
    }

    /**
     * The rows of several record batches (here, different roots with the same schema, as the stream reader reuses one) are appended to
     * a single batch: columns that appear in a later record batch (here, the keys of a map) are absent in the previous rows.
     */
    public void testAppendRecordBatches() throws IOException {
        var entries = new Field(
            "entries",
            FieldType.notNullable(ArrowType.Struct.INSTANCE),
            List.of(new Field("key", FieldType.notNullable(new ArrowType.Utf8()), null), field("value", new ArrowType.Int(32, true)))
        );
        var schema = new Schema(
            List.of(
                field("n", new ArrowType.Int(64, true)),
                field("s", new ArrowType.Utf8()),
                new Field("m", FieldType.nullable(new ArrowType.Map(false)), List.of(entries))
            )
        );

        try (var first = VectorSchemaRoot.create(schema, allocator); var second = VectorSchemaRoot.create(schema, allocator)) {
            // First record batch: two rows, the map of the second row is empty
            ((BigIntVector) first.getVector("n")).setSafe(0, 1);
            ((BigIntVector) first.getVector("n")).setSafe(1, 2);
            ((VarCharVector) first.getVector("s")).setSafe(0, new Text("a"));
            ((VarCharVector) first.getVector("s")).setSafe(1, new Text("b"));
            writeMap(first, 0, Map.of("k", 1));
            writeMap(first, 1, Map.of());
            first.setRowCount(2);

            // Second record batch: three rows, with a null, and a map with a key that wasn't there before
            ((BigIntVector) second.getVector("n")).setSafe(0, 3);
            second.getVector("n").setNull(1);
            ((BigIntVector) second.getVector("n")).setSafe(2, 5);
            for (int i = 0; i < 3; i++) {
                ((VarCharVector) second.getVector("s")).setSafe(i, new Text(List.of("c", "d", "e").get(i)));
            }
            writeMap(second, 0, Map.of("z", 9));
            writeMap(second, 1, Map.of("k", 2));
            writeMap(second, 2, Map.of());
            second.setRowCount(3);

            var conversion = converter.newConversion();
            try {
                conversion.append(first, Map.of(), allColumns(first), null);
                assertThat(conversion.rowCount(), equalTo(2));
                // A record batch without rows changes nothing
                first.setRowCount(0);
                conversion.append(first, Map.of(), allColumns(first), null);
                conversion.append(second, Map.of(), allColumns(second), null);
                assertThat(conversion.rowCount(), equalTo(5));

                try (EscfBatch batch = conversion.build()) {
                    assertThat(batch.docCount(), equalTo(5));
                    assertThat(row(batch, 0), equalTo(Map.of("n", 1, "s", "a", "m", Map.of("k", 1))));
                    assertThat(row(batch, 1), equalTo(Map.of("n", 2, "s", "b")));
                    assertThat(row(batch, 2), equalTo(Map.of("n", 3, "s", "c", "m", Map.of("z", 9))));
                    assertThat(row(batch, 3), equalTo(Map.of("s", "d", "m", Map.of("k", 2))));
                    assertThat(row(batch, 4), equalTo(Map.of("n", 5, "s", "e")));
                }
            } catch (RuntimeException | AssertionError e) {
                conversion.close();
                throw e;
            }
        }
    }

    public void testBuildWithoutRowsIsRejected() {
        var conversion = converter.newConversion();
        try {
            expectThrows(IllegalStateException.class, conversion::build);
        } finally {
            conversion.close();
        }
    }

    private static void writeMap(VectorSchemaRoot root, int position, Map<String, Integer> values) {
        var map = (MapVector) root.getVector("m");
        var writer = map.getWriter();
        writer.setPosition(position);
        writer.startMap();
        for (var entry : values.entrySet()) {
            writer.startEntry();
            writer.key().varChar().writeVarChar(new Text(entry.getKey()));
            writer.value().integer().writeInt(entry.getValue());
            writer.endEntry();
        }
        writer.endMap();
        map.setValueCount(position + 1);
    }

    public void testUnsupportedTypeIsRejected() {
        var schema = new Schema(List.of(field("dec", new ArrowType.Decimal(10, 2, 128))));
        try (var root = VectorSchemaRoot.create(schema, allocator)) {
            ((DecimalVector) root.getVector("dec")).setSafe(0, 100);
            root.setRowCount(1);

            var e = expectThrows(IllegalArgumentException.class, () -> converter.convert(root, Map.of(), allColumns(root), null));
            assertThat(e.getMessage(), containsString("[DECIMAL] of field [dec] is not supported"));
        }
    }
}
