/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.arrow;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.compression.NoCompressionCodec;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.BytesStream;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.rest.ChunkedRestResponseBody;
import org.elasticsearch.xpack.esql.arrow.shim.Shim;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;

public class ArrowResponse {

    public static class Column {
        private final String esqlType;
        private final FieldType arrowType;
        private final String name;

        public Column(String esqlType, String name) {
            this.esqlType = esqlType;
            this.arrowType = arrowFieldType(esqlType);
            this.name = name;
        }

        String esqlType() {
            return esqlType;
        }

        Field arrowField() {
            return new Field(name, arrowType, List.of());
        }
    }

    private final List<Column> columns;
    private final List<Page> pages;

    public ArrowResponse(List<Column> columns, List<Page> pages) {
        this.columns = columns;
        this.pages = pages;
    }

    List<Column> columns() {
        return columns;
    }

    List<Page> pages() {
        return pages;
    }

    public ChunkedRestResponseBody.FromMany chunkedResponse() {
        // TODO dictionaries

        SchemaResponse schemaResponse = new SchemaResponse(this);
        List<ChunkedRestResponseBody> rest = new ArrayList<>(pages.size());
        for (int p = 0; p < pages.size(); p++) {
            rest.add(new PageResponse(this, pages.get(p)));
        }
        rest.add(new EndResponse(this));

        return ChunkedRestResponseBody.fromMany(schemaResponse, rest.iterator());
    }

    protected abstract static class AbstractArrowChunkedResponse implements ChunkedRestResponseBody {
        static {
            // Init the arrow shim
            Shim.init();
        }

        protected final ArrowResponse response;

        AbstractArrowChunkedResponse(ArrowResponse response) {
            this.response = response;
        }

        @Override
        public final ReleasableBytesReference encodeChunk(int sizeHint, Recycler<BytesRef> recycler) throws IOException {
            RecyclerBytesStreamOutput output = new RecyclerBytesStreamOutput(recycler);
            try {
                encodeChunk(sizeHint, output);
                BytesReference ref = output.bytes();
                RecyclerBytesStreamOutput closeRef = output;
                output = null;
                ReleasableBytesReference result = new ReleasableBytesReference(ref, () -> Releasables.closeExpectNoException(closeRef));
                return result;
            } catch (Exception e) {
                logger.error("failed to write arrow chunk", e);
                throw e;
            } finally {
                if (output != null) {
                    // assert false : "failed to write arrow chunk";
                    Releasables.closeExpectNoException(output);
                }
            }
        }

        protected abstract void encodeChunk(int sizeHint, RecyclerBytesStreamOutput out) throws IOException;

        /**
         * Adapts a {@link BytesStream} so that Arrow can write to it.
         */
        protected static WritableByteChannel arrowOut(BytesStream output) {
            return new WritableByteChannel() {
                @Override
                public int write(ByteBuffer byteBuffer) throws IOException {
                    if (byteBuffer.hasArray() == false) {
                        throw new AssertionError("only implemented for array backed buffers");
                    }
                    int length = byteBuffer.remaining();
                    output.write(byteBuffer.array(), byteBuffer.arrayOffset() + byteBuffer.position(), length);
                    byteBuffer.position(byteBuffer.position() + length);
                    assert byteBuffer.hasRemaining() == false;
                    return length;
                }

                @Override
                public boolean isOpen() {
                    return true;
                }

                @Override
                public void close() {}
            };
        }

        @Override
        public final String getResponseContentTypeString() {
            return ArrowFormat.CONTENT_TYPE;
        }
    }

    /**
     * Header part of the Arrow response containing the dataframe schema.
     *
     * @see <a href="https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format">IPC Streaming Format</a>
     */
    private static class SchemaResponse extends AbstractArrowChunkedResponse {
        private boolean done = false;

        SchemaResponse(ArrowResponse response) {
            super(response);
        }

        @Override
        public boolean isDone() {
            return done;
        }

        @Override
        protected void encodeChunk(int sizeHint, RecyclerBytesStreamOutput out) throws IOException {
            WriteChannel arrowOut = new WriteChannel(arrowOut(out));
            MessageSerializer.serialize(arrowOut, arrowSchema());
            done = true;
        }

        private Schema arrowSchema() {
            return new Schema(response.columns.stream().map(ArrowResponse.Column::arrowField).toList());
        }
    }

    /**
     * Write an ES|QL page as an Arrow RecordBatch
     */
    private static class PageResponse extends AbstractArrowChunkedResponse {
        private final Page page;
        private boolean done = false;

        PageResponse(ArrowResponse response, Page page) {
            super(response);
            this.page = page;
        }

        @Override
        public boolean isDone() {
            return done;
        }

        // Writes some data and returns the number of bytes written.
        interface BufWriter {
            long write() throws IOException;
        }

        @Override
        protected void encodeChunk(int sizeHint, RecyclerBytesStreamOutput out) throws IOException {
            // An Arrow record batch consists of:
            // - fields metadata, giving the number of items and the number of null values for each field
            // - data buffers for each field. The number of buffers for a field depends on its type, e.g.:
            //   - for primitive types, there's a validity buffer (for nulls) and a value buffer.
            //   - for strings, there's a validity buffer, an offsets buffer and a data buffer
            //
            // See https://arrow.apache.org/docs/format/Columnar.html#recordbatch-message

            // Field metadata
            List<ArrowFieldNode> nodes = new ArrayList<>(page.getBlockCount());

            // Buffers added to the record batch. They're used to track data size so that Arrow can compute offsets
            // but contain no data. Actual writing will be done by the bufWriters. This avoids having to deal with
            // Arrow's memory management, and in the future will allow direct write from ESQL block vectors.
            List<ArrowBuf> bufs = new ArrayList<>(page.getBlockCount() * 2);

            // Closures that will actually write a Block's data. Maps 1:1 to `bufs`.
            List<BufWriter> bufWriters = new ArrayList<>(page.getBlockCount() * 2);

            // Give Arrow a WriteChannel that will iterate on `bufWriters` when requested to write a buffer.
            WriteChannel arrowOut = new WriteChannel(arrowOut(out)) {
                int bufIdx = 0;
                long extraPosition = 0;

                @Override
                public void write(ArrowBuf buffer) throws IOException {
                    extraPosition += bufWriters.get(bufIdx++).write();
                }

                @Override
                public long getCurrentPosition() {
                    return super.getCurrentPosition() + extraPosition;
                }

                @Override
                public long align() throws IOException {
                    int trailingByteSize = (int) (getCurrentPosition() % 8);
                    if (trailingByteSize != 0) { // align on 8 byte boundaries
                        return writeZeros(8 - trailingByteSize);
                    }
                    return 0;
                }
            };

            // Create Arrow buffers for each of the blocks in this page
            for (int b = 0; b < page.getBlockCount(); b++) {
                accumulateBlock(out, nodes, bufs, bufWriters, page.getBlock(b));
            }

            // Create the batch and serialize it
            ArrowRecordBatch batch = new ArrowRecordBatch(
                page.getPositionCount(),
                nodes,
                bufs,
                NoCompressionCodec.DEFAULT_BODY_COMPRESSION,
                true, // align buffers
                false // retain buffers
            );
            MessageSerializer.serialize(arrowOut, batch);

            done = true; // one day we should respect sizeHint here. kindness.
        }

        // Length in bytes of a validity buffer
        private static int validityLength(int totalValues) {
            return (totalValues - 1) / Byte.SIZE + 1;
        }

        // Block.nullValuesCount was more efficient but was removed in https://github.com/elastic/elasticsearch/pull/108916
        private int nullValuesCount(Block block) {
            if (block.mayHaveNulls() == false) {
                return 0;
            }
            int count = 0;
            for (int i = 0; i < block.getPositionCount(); i++) {
                if (block.isNull(i)) {
                    count++;
                }
            }
            return count;
        }

        private void accumulateBlock(
            RecyclerBytesStreamOutput out,
            List<ArrowFieldNode> nodes,
            List<ArrowBuf> bufs,
            List<BufWriter> bufWriters,
            Block block
        ) {
            nodes.add(new ArrowFieldNode(block.getPositionCount(), nullValuesCount(block)));
            switch (block.elementType()) {
                case DOUBLE -> {
                    DoubleBlock b = (DoubleBlock) block;
                    DoubleVector v = b.asVector();
                    if (v != null) {
                        accumulateVectorValidity(out, bufs, bufWriters, b);
                        bufs.add(dummyArrowBuf(vectorLength(v)));
                        bufWriters.add(() -> writeVector(out, v));
                        return;
                    }
                    throw new UnsupportedOperationException();
                }
                case INT -> {
                    IntBlock b = (IntBlock) block;
                    IntVector v = b.asVector();
                    if (v != null) {
                        accumulateVectorValidity(out, bufs, bufWriters, b);
                        bufs.add(dummyArrowBuf(vectorLength(v)));
                        bufWriters.add(() -> writeVector(out, v));
                        return;
                    }
                    throw new UnsupportedOperationException();
                }
                case LONG -> {
                    LongBlock b = (LongBlock) block;

                    LongVector v = b.asVector();
                    if (v != null) {
                        accumulateVectorValidity(out, bufs, bufWriters, b);
                        bufs.add(dummyArrowBuf(vectorLength(v)));
                        bufWriters.add(() -> writeVector(out, v));
                        return;
                    }
                    throw new UnsupportedOperationException();
                }
                case BYTES_REF -> {
                    BytesRefBlock b = (BytesRefBlock) block;
                    BytesRefVector v = b.asVector();
                    if (v != null) {
                        accumulateVectorValidity(out, bufs, bufWriters, b);
                        bufs.add(dummyArrowBuf(vectorOffsetLength(v)));
                        bufWriters.add(() -> writeVectorOffset(out, v));
                        bufs.add(dummyArrowBuf(vectorLength(v)));
                        bufWriters.add(() -> writeVector(out, v));
                        return;
                    }
                    throw new UnsupportedOperationException();
                }
                default -> {
                    throw new UnsupportedOperationException("ES|QL block type [" + block.elementType() + "] not supported by Arrow format");
                }
            }
        }

        private void accumulateVectorValidity(RecyclerBytesStreamOutput out, List<ArrowBuf> bufs, List<BufWriter> bufWriters, Block b) {
            bufs.add(dummyArrowBuf(validityLength(b.getPositionCount())));
            bufWriters.add(() -> writeAllTrueValidity(out, b.getPositionCount()));
        }

        private long writeAllTrueValidity(RecyclerBytesStreamOutput out, int valueCount) {
            int allOnesCount = valueCount / 8;
            for (int i = 0; i < allOnesCount; i++) {
                out.writeByte((byte) 0xff);
            }
            int remaining = valueCount % 8;
            if (remaining == 0) {
                return allOnesCount;
            }
            out.writeByte((byte) ((1 << remaining) - 1));
            return allOnesCount + 1;
        }

        private long vectorLength(IntVector vector) {
            return Integer.BYTES * vector.getPositionCount();
        }

        private long writeVector(RecyclerBytesStreamOutput out, IntVector vector) throws IOException {
            // TODO could we "just" get the memory of the array and dump it?
            for (int i = 0; i < vector.getPositionCount(); i++) {
                out.writeIntLE(vector.getInt(i));
            }
            return vectorLength(vector);
        }

        private long vectorLength(LongVector vector) {
            return Long.BYTES * vector.getPositionCount();
        }

        private long writeVector(RecyclerBytesStreamOutput out, LongVector vector) throws IOException {
            // TODO could we "just" get the memory of the array and dump it?
            for (int i = 0; i < vector.getPositionCount(); i++) {
                out.writeLongLE(vector.getLong(i));
            }
            return vectorLength(vector);
        }

        private long vectorLength(DoubleVector vector) {
            return Double.BYTES * vector.getPositionCount();
        }

        private long writeVector(RecyclerBytesStreamOutput out, DoubleVector vector) throws IOException {
            // TODO could we "just" get the memory of the array and dump it?
            for (int i = 0; i < vector.getPositionCount(); i++) {
                out.writeDoubleLE(vector.getDouble(i));
            }
            return vectorLength(vector);
        }

        private long vectorOffsetLength(BytesRefVector vector) {
            return Integer.BYTES * (vector.getPositionCount() + 1);
        }

        private long writeVectorOffset(RecyclerBytesStreamOutput out, BytesRefVector vector) throws IOException {
            // TODO could we "just" get the memory of the array and dump it?
            BytesRef scratch = new BytesRef();
            int offset = 0;
            for (int i = 0; i < vector.getPositionCount(); i++) {
                out.writeIntLE(offset);
                offset += vector.getBytesRef(i, scratch).length;
            }
            out.writeIntLE(offset);
            return vectorOffsetLength(vector);
        }

        private long vectorLength(BytesRefVector vector) {
            // TODO we can probably get the length from the vector without all this sum - it's in an array
            long length = 0;
            BytesRef scratch = new BytesRef();
            for (int i = 0; i < vector.getPositionCount(); i++) {
                length += vector.getBytesRef(i, scratch).length;
            }
            return length;
        }

        private long writeVector(RecyclerBytesStreamOutput out, BytesRefVector vector) throws IOException {
            // TODO could we "just" get the memory of the array and dump it?
            BytesRef scratch = new BytesRef();
            long length = 0;
            for (int i = 0; i < vector.getPositionCount(); i++) {
                BytesRef v = vector.getBytesRef(i, scratch);
                out.write(v.bytes, v.offset, v.length);
                length += v.length;
            }
            return length;
        }

        // Create a dummy ArrowBuf used for size accounting purposes.
        private ArrowBuf dummyArrowBuf(long size) {
            return new ArrowBuf(null, null, 0, 0).writerIndex(size);
        }
    }

    private class EndResponse extends AbstractArrowChunkedResponse {
        private boolean done = false;

        private EndResponse(ArrowResponse response) {
            super(response);
        }

        @Override
        public boolean isDone() {
            return done;
        }

        @Override
        protected void encodeChunk(int sizeHint, RecyclerBytesStreamOutput out) throws IOException {
            ArrowStreamWriter.writeEndOfStream(new WriteChannel(arrowOut(out)), IpcOption.DEFAULT);
            done = true;
        }
    }

    static FieldType arrowFieldType(String fieldType) {
        return switch (fieldType) {
            case "date" -> FieldType.nullable(Types.MinorType.DATEMILLI.getType());
            case "double" -> FieldType.nullable(Types.MinorType.FLOAT8.getType());
            case "integer" -> FieldType.nullable(Types.MinorType.INT.getType());
            case "long" -> FieldType.nullable(Types.MinorType.BIGINT.getType());
            case "keyword", "text" -> FieldType.nullable(Types.MinorType.VARCHAR.getType());
            default -> throw new UnsupportedOperationException("Field type [" + fieldType + "] not supported by Arrow format");
        };
    }
}
