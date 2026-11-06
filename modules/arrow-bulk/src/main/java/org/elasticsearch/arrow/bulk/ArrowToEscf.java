/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.arrow.bulk;

import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.Float2Vector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VariableWidthFieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.LargeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.util.ReusableByteArray;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.escf.EscfBatch;
import org.elasticsearch.escf.EscfColumnBuilder;
import org.elasticsearch.escf.EscfColumnData;
import org.elasticsearch.sourcebatch.SourceBatchEncodeHelper;
import org.elasticsearch.sourcebatch.SourceSchema;
import org.elasticsearch.sourcebatch.SourceValueType;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.IntPredicate;
import java.util.function.IntToDoubleFunction;
import java.util.function.IntToLongFunction;

/**
 * Converts rows of an Arrow {@link VectorSchemaRoot} to an {@link EscfBatch}.
 * <p>
 * Both formats are columnar, so the conversion is column-major: every Arrow vector is read in a single typed loop and written
 * directly to the {@link EscfColumnBuilder} of the matching ESCF column, without going through a per-row representation.
 * The resulting batch is attached to a bulk request, and is routed, mapped and indexed through the columnar indexing path.
 * <p>
 * Type mapping (the same value conversions as the former XContent-based Arrow ingestion, so both give the same documents):
 * <ul>
 * <li>integers (signed and unsigned) are stored as longs, floating-point numbers as doubles, bits as booleans;
 * <li>strings (including dictionary-encoded ones) are stored as UTF-8 strings;
 * <li>timestamps, dates and times are stored as epoch milliseconds (seconds and milliseconds) or nanoseconds
 * (microseconds and nanoseconds), time zones are dropped;
 * <li>structs are stored as objects, and maps with string keys and scalar values as objects too;
 * <li>lists of scalar values are stored as arrays.
 * </ul>
 * Arrow nulls (cleared validity bits) are stored as absent values: this keeps columns homogeneous and compact, and is
 * how columnar sources usually express missing data. Nulls inside lists are dropped, and the children of a null struct are absent.
 * <p>
 * Types that cannot (yet) be expressed in an ESCF batch (decimals, binary, unions, run-end-encoded vectors, nested lists,
 * extension types) are rejected with an {@link IllegalArgumentException} rather than being silently converted to something else.
 *
 * @see <a href="https://arrow.apache.org/docs/format/Columnar.html#data-types">Arrow data types</a>
 */
final class ArrowToEscf {

    /** The schema index of the document root. */
    private static final int ROOT = 0;

    private final Recycler<BytesRef> recycler;

    ArrowToEscf(Recycler<BytesRef> recycler) {
        this.recycler = recycler;
    }

    /**
     * Converts the given rows of {@code root} to a batch.
     *
     * @param root the Arrow data
     * @param dictionaries to look up the values of dictionary-encoded vectors
     * @param valueColumns the indices of the vectors that are document fields (as opposed to bulk metadata like {@code _id})
     * @param rows the rows to convert, in order. The row {@code i} of the resulting batch is {@code rows[i]}. If {@code null}, all
     *             the rows of {@code root} are converted.
     * @return a batch that must be closed by the caller
     */
    EscfBatch convert(VectorSchemaRoot root, Map<Long, Dictionary> dictionaries, BitSet valueColumns, @Nullable int[] rows) {
        Conversion conversion = new Conversion();
        try {
            conversion.append(root, dictionaries, valueColumns, rows);
            return conversion.build();
        } catch (RuntimeException e) {
            conversion.close();
            throw e;
        }
    }

    /**
     * Starts a conversion to which the rows of several record batches can be appended, to build a single batch that is bigger than
     * a record batch. The caller must either {@link Conversion#build() build} the batch or {@link Conversion#close() close} it.
     */
    Conversion newConversion() {
        return new Conversion();
    }

    /**
     * Returns the value of a scalar vector position as a string, for metadata columns such as {@code _id} or {@code _index}.
     * Returns {@code null} if the value is null.
     */
    @Nullable
    String stringValue(ValueVector vector, int position, Map<Long, Dictionary> dictionaries) {
        var dictEncoding = vector.getField().getDictionary();
        if (dictEncoding != null) {
            if (vector.isNull(position)) {
                return null;
            }
            Dictionary dictionary = dictionaries.get(dictEncoding.getId());
            if (dictionary == null) {
                throw new IllegalArgumentException(
                    "Missing dictionary [" + dictEncoding.getId() + "] for field [" + vector.getName() + "]"
                );
            }
            position = (int) ((BaseIntVector) vector).getValueAsLong(position);
            vector = dictionary.getVector();
        }
        if (vector.isNull(position)) {
            return null;
        }
        return switch (vector.getMinorType()) {
            case VARCHAR, LARGEVARCHAR, VIEWVARCHAR -> {
                var buffer = new ReusableByteArray();
                ((VariableWidthFieldVector) vector).read(position, buffer);
                yield new String(buffer.getBuffer(), 0, (int) buffer.getLength(), StandardCharsets.UTF_8);
            }
            case TINYINT, SMALLINT, INT, BIGINT, UINT1, UINT2, UINT4 -> Long.toString(((BaseIntVector) vector).getValueAsLong(position));
            default -> throw new IllegalArgumentException(
                "Field [" + vector.getName() + "] must be a string or an integer, but was [" + vector.getMinorType() + "]"
            );
        };
    }

    /** The state of the conversion of one batch: the schema being discovered, and one column builder per leaf. */
    final class Conversion implements Releasable {
        /** The number of rows appended so far. */
        private int rowCount;
        /** The row of the batch to which the first row of the record batch that is being appended goes. */
        private int base;
        /** The dictionaries of the record batch that is being appended: they can change from one record batch to the next. */
        private Map<Long, Dictionary> dictionaries;
        private final SourceSchema schema = new SourceSchema();
        private final List<EscfColumnBuilder> builders = new ArrayList<>();
        private final ReusableByteArray bytesBuffer = new ReusableByteArray();
        private final BytesRef bytesRef = new BytesRef();

        private Conversion() {}

        /** The number of rows appended so far. */
        int rowCount() {
            return rowCount;
        }

        /**
         * Appends rows of a record batch. The columns of the batch are the ones of the previous calls: the column builders and the
         * schema are shared, and the rows are written after the ones that were appended before.
         *
         * @param root the Arrow data. Its vectors are only read during this call, so they can be reused afterwards
         * @param dictionaries to look up the values of dictionary-encoded vectors
         * @param valueColumns the indices of the vectors that are document fields (as opposed to bulk metadata like {@code _id})
         * @param rows the rows to append, in order, or {@code null} to append all the rows of {@code root}
         */
        void append(VectorSchemaRoot root, Map<Long, Dictionary> dictionaries, BitSet valueColumns, @Nullable int[] rows) {
            int count = rows == null ? root.getRowCount() : rows.length;
            if (count == 0) {
                return;
            }
            int[] positions = rows;
            if (positions == null) {
                positions = new int[count];
                for (int i = 0; i < count; i++) {
                    positions[i] = i;
                }
            }
            this.dictionaries = dictionaries;
            this.base = rowCount;
            for (int i = valueColumns.nextSetBit(0); i >= 0; i = valueColumns.nextSetBit(i + 1)) {
                ValueVector vector = root.getVector(i);
                writeColumn(ROOT, vector.getName(), vector, positions);
            }
            rowCount += count;
        }

        /** Builds the batch of all the rows that were appended. The conversion can't be used afterwards. */
        EscfBatch build() {
            if (rowCount == 0) {
                throw new IllegalStateException("cannot build a batch without rows");
            }
            int leafCount = schema.leafCount();
            EscfColumnData[] columns = new EscfColumnData[leafCount];
            try {
                for (int i = 0; i < leafCount; i++) {
                    columns[i] = builder(i).finish(rowCount);
                }
            } catch (RuntimeException e) {
                Releasables.close(columns);
                throw e;
            }
            // Each column owns its recycler-backed buffers; the batch releases them all on close.
            return EscfBatch.of(schema, rowCount, columns, Releasables.wrap(columns));
        }

        @Override
        public void close() {
            Releasables.close(builders);
        }

        private EscfColumnBuilder builder(int leaf) {
            while (builders.size() <= leaf) {
                builders.add(new EscfColumnBuilder(EscfColumnBuilder.CollisionPolicy.SPLIT, recycler));
            }
            return builders.get(leaf);
        }

        /**
         * Writes a vector to the column(s) below {@code parent}.
         *
         * @param positions for each row of the batch, the position in {@code vector} of its value, or -1 if it has none
         */
        void writeColumn(int parent, String name, ValueVector vector, int[] positions) {
            var field = vector.getField();
            var extension = field.getMetadata().get(ArrowType.ExtensionType.EXTENSION_METADATA_KEY_NAME);
            if (extension != null) {
                // E.g. arrow.json, uuid, tensors, GeoArrow
                throw new IllegalArgumentException(
                    "Field [" + name + "] has the Arrow extension type [" + extension + "] that isn't supported"
                );
            }

            var dictEncoding = field.getDictionary();
            if (dictEncoding != null) {
                // Read the values from the dictionary, through the indices stored in the vector.
                Dictionary dictionary = dictionaries.get(dictEncoding.getId());
                if (dictionary == null) {
                    throw new IllegalArgumentException("Missing dictionary [" + dictEncoding.getId() + "] for field [" + name + "]");
                }
                var indices = (BaseIntVector) vector;
                int[] dictPositions = new int[positions.length];
                for (int row = 0; row < positions.length; row++) {
                    int pos = positions[row];
                    // The spec allows any integer type, although signed 32 bits are recommended
                    dictPositions[row] = pos < 0 || indices.isNull(pos) ? -1 : (int) indices.getValueAsLong(pos);
                }
                vector = dictionary.getVector();
                positions = dictPositions;
            }

            switch (vector.getMinorType()) {
                case STRUCT -> writeStruct(parent, name, (StructVector) vector, positions);
                case MAP -> writeMap(parent, name, (MapVector) vector, positions);
                case LIST, LARGELIST, FIXED_SIZE_LIST -> writeList(parent, name, vector, positions);
                default -> writeScalar(parent, name, vector, positions);
            }
        }

        private void writeScalar(int parent, String name, ValueVector vector, int[] positions) {
            Accessor accessor = accessor(name, vector);
            EscfColumnBuilder builder = builder(schema.appendLeaf(name, parent));
            // Rows without a value are skipped: the builder fills the gaps with absent values.
            for (int row = 0; row < positions.length; row++) {
                int pos = positions[row];
                if (pos >= 0 && vector.isNull(pos) == false) {
                    setValue(builder, row, accessor, pos);
                }
            }
        }

        private void setValue(EscfColumnBuilder builder, int row, Accessor accessor, int pos) {
            switch (accessor.kind) {
                case LONG -> builder.setLong(base + row, accessor.longs.applyAsLong(pos));
                case DOUBLE -> builder.setDouble(base + row, accessor.doubles.applyAsDouble(pos));
                case BOOLEAN -> builder.setBoolean(base + row, accessor.booleans.test(pos));
                case STRING -> {
                    // Builders copy the bytes, so the read buffer can be reused.
                    accessor.strings.read(pos, bytesBuffer);
                    builder.setString(base + row, bytesBuffer.getBuffer(), 0, (int) bytesBuffer.getLength());
                }
            }
        }

        private void writeStruct(int parent, String name, StructVector vector, int[] positions) {
            int object = schema.appendNonLeaf(name, parent);
            // The children of a null struct may hold anything: its rows have no value.
            int[] childPositions = vector.getNullCount() == 0 ? positions : maskNulls(vector, positions);
            for (ValueVector child : vector.getChildrenFromFields()) {
                writeColumn(object, child.getName(), child, childPositions);
            }
        }

        private void writeMap(int parent, String name, MapVector vector, int[] positions) {
            var entries = (StructVector) vector.getDataVector();
            ValueVector keys = entries.getChildrenFromFields().get(0);
            ValueVector values = entries.getChildrenFromFields().get(1);
            if (keys.getField().getDictionary() != null
                || values.getField().getDictionary() != null
                || values.getField().getChildren().isEmpty() == false) {
                throw new IllegalArgumentException(
                    "Field ["
                        + name
                        + "] is a map of ["
                        + values.getMinorType()
                        + "], but only maps of non-dictionary scalar values are supported"
                );
            }

            int object = schema.appendNonLeaf(name, parent);
            Accessor accessor = accessor(name, values);
            Map<String, Integer> keyColumns = new HashMap<>();
            for (int row = 0; row < positions.length; row++) {
                int pos = positions[row];
                if (pos < 0 || vector.isNull(pos)) {
                    continue;
                }
                for (int entry = vector.getElementStartIndex(pos); entry < vector.getElementEndIndex(pos); entry++) {
                    String key = stringValue(keys, entry, dictionaries);
                    if (key == null) {
                        throw new IllegalArgumentException("Map keys of field [" + name + "] cannot be null");
                    }
                    if (values.isNull(entry) == false) {
                        int leaf = keyColumns.computeIfAbsent(key, k -> schema.appendLeaf(k, object));
                        setValue(builder(leaf), row, accessor, entry);
                    }
                }
            }
        }

        private void writeList(int parent, String name, ValueVector vector, int[] positions) {
            final ValueVector elements = switch (vector) {
                case ListVector list -> list.getDataVector();
                case LargeListVector list -> list.getDataVector();
                case FixedSizeListVector list -> list.getDataVector();
                default -> throw unsupported(name, vector);
            };
            if (elements.getField().getDictionary() != null || elements.getField().getChildren().isEmpty() == false) {
                throw new IllegalArgumentException(
                    "Field [" + name + "] is a list of [" + elements.getMinorType() + "], but only lists of scalar values are supported"
                );
            }

            Accessor accessor = accessor(name, elements);
            EscfColumnBuilder builder = builder(schema.appendLeaf(name, parent));
            for (int row = 0; row < positions.length; row++) {
                int pos = positions[row];
                if (pos < 0 || vector.isNull(pos)) {
                    continue;
                }
                final int start;
                final int end;
                switch (vector) {
                    case ListVector list -> {
                        start = list.getElementStartIndex(pos);
                        end = list.getElementEndIndex(pos);
                    }
                    case LargeListVector list -> {
                        start = Math.toIntExact(list.getElementStartIndex(pos));
                        end = Math.toIntExact(list.getElementEndIndex(pos));
                    }
                    case FixedSizeListVector list -> {
                        start = list.getElementStartIndex(pos);
                        end = list.getElementEndIndex(pos);
                    }
                    default -> throw unsupported(name, vector);
                }

                if (accessor.kind == Kind.BOOLEAN) {
                    // There's no columnar representation of boolean arrays: they're stored inline.
                    writeBooleanArray(builder, row, elements, accessor, start, end);
                    continue;
                }

                builder.beginArray(base + row);
                for (int i = start; i < end; i++) {
                    if (elements.isNull(i)) {
                        continue;
                    }
                    switch (accessor.kind) {
                        case LONG -> builder.appendLong(accessor.longs.applyAsLong(i));
                        case DOUBLE -> builder.appendDouble(accessor.doubles.applyAsDouble(i));
                        case STRING -> {
                            accessor.strings.read(i, bytesBuffer);
                            bytesRef.bytes = bytesBuffer.getBuffer();
                            bytesRef.offset = 0;
                            bytesRef.length = (int) bytesBuffer.getLength();
                            builder.appendString(bytesRef);
                        }
                        case BOOLEAN -> throw new AssertionError("handled above");
                    }
                }
                builder.endArray();
            }
        }

        private void writeBooleanArray(EscfColumnBuilder builder, int row, ValueVector elements, Accessor accessor, int start, int end) {
            byte[] types = new byte[end - start];
            int count = 0;
            for (int i = start; i < end; i++) {
                if (elements.isNull(i) == false) {
                    types[count++] = accessor.booleans.test(i) ? SourceValueType.TRUE : SourceValueType.FALSE;
                }
            }
            // This is an "append" operation, so rows without a value must be filled first
            builder.addAbsents(base + row - builder.nextAppendRow());
            byte[] packed = SourceBatchEncodeHelper.packUnionArray(types, new long[count], new Object[count], count);
            builder.addArray(SourceValueType.UNION_ARRAY, packed);
        }
    }

    /** Returns {@code positions} where rows that are null in {@code vector} are replaced by -1. */
    private static int[] maskNulls(ValueVector vector, int[] positions) {
        int[] masked = new int[positions.length];
        for (int row = 0; row < positions.length; row++) {
            int pos = positions[row];
            masked[row] = pos < 0 || vector.isNull(pos) ? -1 : pos;
        }
        return masked;
    }

    /** Creates the accessor of a scalar vector, which provides its values converted to what ESCF stores. */
    private static Accessor accessor(String name, ValueVector vector) {
        return switch (vector.getMinorType()) {
            // ---- Numbers
            case TINYINT -> longs(((TinyIntVector) vector)::get);
            case SMALLINT -> longs(((SmallIntVector) vector)::get);
            case INT -> longs(((IntVector) vector)::get);
            case BIGINT -> longs(((BigIntVector) vector)::get);
            case UINT1 -> longs(((UInt1Vector) vector)::getValueAsLong);
            case UINT2 -> longs(((UInt2Vector) vector)::get);
            // getValueAsLong() to keep unsigned values greater than 0x7FFF_FFFF positive
            case UINT4 -> longs(((UInt4Vector) vector)::getValueAsLong);
            case UINT8 -> longs(((UInt8Vector) vector)::get);
            case FLOAT2 -> doubles(((Float2Vector) vector)::getValueAsFloat);
            case FLOAT4 -> doubles(((Float4Vector) vector)::get);
            case FLOAT8 -> doubles(((Float8Vector) vector)::get);

            // ---- Booleans
            case BIT -> booleans(pos -> ((BitVector) vector).get(pos) != 0);

            // ---- Strings
            case VARCHAR, LARGEVARCHAR, VIEWVARCHAR -> new Accessor(Kind.STRING, null, null, null, (VariableWidthFieldVector) vector);

            // ---- Timestamps, dates and times
            //
            // Seconds and millis are stored as millis, and micros and nanos as nanos, so that there's no precision loss.
            // The ES date type has no timezone, so we drop this information.
            case TIMESTAMPSEC, TIMESTAMPSECTZ, TIMESTAMPMICRO, TIMESTAMPMICROTZ -> longs(
                pos -> ((TimeStampVector) vector).get(pos) * 1000L
            );
            case TIMESTAMPMILLI, TIMESTAMPMILLITZ, TIMESTAMPNANO, TIMESTAMPNANOTZ -> longs(((TimeStampVector) vector)::get);
            case DATEDAY -> longs(pos -> ((DateDayVector) vector).get(pos) * 86_400_000L);
            case DATEMILLI -> longs(((DateMilliVector) vector)::get);
            case TIMESEC -> longs(pos -> ((TimeSecVector) vector).get(pos) * 1000L);
            case TIMEMILLI -> longs(((TimeMilliVector) vector)::get);
            case TIMEMICRO -> longs(pos -> ((TimeMicroVector) vector).get(pos) * 1000L);
            case TIMENANO -> longs(((TimeNanoVector) vector)::get);

            default -> throw unsupported(name, vector);
        };
    }

    private static IllegalArgumentException unsupported(String name, ValueVector vector) {
        return new IllegalArgumentException(
            "Arrow type [" + vector.getMinorType() + "] of field [" + name + "] is not supported for ingestion"
        );
    }

    private enum Kind {
        LONG,
        DOUBLE,
        BOOLEAN,
        STRING
    }

    private static Accessor longs(IntToLongFunction values) {
        return new Accessor(Kind.LONG, values, null, null, null);
    }

    private static Accessor doubles(IntToDoubleFunction values) {
        return new Accessor(Kind.DOUBLE, null, values, null, null);
    }

    private static Accessor booleans(IntPredicate values) {
        return new Accessor(Kind.BOOLEAN, null, null, values, null);
    }

    /**
     * Reads the non-null values of a scalar vector, once converted to what ESCF stores. Exactly one of the accessors matching
     * the {@link #kind} is set. This resolves the vector type once per column rather than once per value.
     */
    private record Accessor(
        Kind kind,
        @Nullable IntToLongFunction longs,
        @Nullable IntToDoubleFunction doubles,
        @Nullable IntPredicate booleans,
        @Nullable VariableWidthFieldVector strings
    ) {}
}
