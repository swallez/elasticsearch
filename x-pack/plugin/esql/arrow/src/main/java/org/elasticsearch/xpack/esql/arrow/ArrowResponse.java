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
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.BytesStream;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.rest.ChunkedRestResponseBody;
import org.elasticsearch.xpack.esql.arrow.shim.Shim;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ArrowResponse {

    public static class Column {
        private final String esqlType;
        private final BlockConverter converter;
        private final String name;

        public Column(String esqlType, String name) {
            this.esqlType = esqlType;
            this.converter = ESQL_CONVERTERS.get(esqlType);
            if (converter == null) {
                throw new IllegalArgumentException("ES|QL type [" + esqlType + "] is not supported by the Arrow format");
            }
            this.name = name;
        }

        String esqlType() {
            return esqlType;
        }

        Field arrowField() {
            return new Field(name, converter.arrowFieldType(), List.of());
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
            // See https://arrow.apache.org/docs/format/Columnar.html#recordbatch-message

            // Field metadata
            List<ArrowFieldNode> nodes = new ArrayList<>(page.getBlockCount());

            // Buffers added to the record batch. They're used to track data size so that Arrow can compute offsets
            // but contain no data. Actual writing will be done by the bufWriters. This avoids having to deal with
            // Arrow's memory management, and in the future will allow direct write from ESQL block vectors.
            List<ArrowBuf> bufs = new ArrayList<>(page.getBlockCount() * 2);

            // Closures that will actually write a Block's data. Maps 1:1 to `bufs`.
            List<BlockConverter.BufWriter> bufWriters = new ArrayList<>(page.getBlockCount() * 2);

            // Give Arrow a WriteChannel that will iterate on `bufWriters` when requested to write a buffer.
            WriteChannel arrowOut = new WriteChannel(arrowOut(out)) {
                int bufIdx = 0;
                long extraPosition = 0;

                @Override
                public void write(ArrowBuf buffer) throws IOException {
                    extraPosition += bufWriters.get(bufIdx++).write(out);
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
                var converter = response.columns.get(b).converter;

                Block block = page.getBlock(b);
                nodes.add(new ArrowFieldNode(block.getPositionCount(), converter.nullValuesCount(block)));
                converter.convert(block, bufs, bufWriters);
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
    }

    private static class EndResponse extends AbstractArrowChunkedResponse {
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

    /**
     * Converters for every ES|QL type
     */
    private static final Map<String, BlockConverter> ESQL_CONVERTERS = Map.ofEntries(
        // For reference, JSON conversions are in PositionToXContent
        // Missing: multi-valued values

        buildEntry(new BlockConverter.AsNull("null")),
        buildEntry(new BlockConverter.AsNull("unsupported")),

        buildEntry(new BlockConverter.AsBoolean("boolean")),

        buildEntry(new BlockConverter.AsInt32("integer")),
        buildEntry(new BlockConverter.AsInt32("counter_integer")),

        buildEntry(new BlockConverter.AsInt64("long")),
        // FIXME: counters: are they signed?
        buildEntry(new BlockConverter.AsInt64("counter_long")),
        buildEntry(new BlockConverter.AsInt64("unsigned_long", MinorType.UINT8)),

        buildEntry(new BlockConverter.AsFloat64("double")),
        buildEntry(new BlockConverter.AsFloat64("counter_double")),

        buildEntry(new BlockConverter.AsVarChar("keyword")),
        buildEntry(new BlockConverter.AsVarChar("text")),

        // date: array of int64 seconds since epoch
        // FIXME: is it signed?
        buildEntry(new BlockConverter.AsInt64("date", MinorType.TIMESTAMPMILLI)),

        // ip are represented as 16-byte ipv6 addresses. We shorten mapped ipv4 addresses to 4 bytes.
        // Another option would be to use a fixed size binary to avoid the offset array. But with mostly
        // ipv4 addresses it would still be twice as big.
        buildEntry(new BlockConverter.TransformedBytesRef("ip", MinorType.VARBINARY, ValueConversions::shortenIpV4Addresses)),

        // geo_point: Keep WKB format (JSON converts to WKT)
        buildEntry(new BlockConverter.AsVarBinary("geo_point")),
        buildEntry(new BlockConverter.AsVarBinary("geo_shape")),
        buildEntry(new BlockConverter.AsVarBinary("cartesian_point")),
        buildEntry(new BlockConverter.AsVarBinary("cartesian_shape")),

        // version: convert to string
        buildEntry(new BlockConverter.TransformedBytesRef("version", MinorType.VARCHAR, ValueConversions::versionToString)),

        // _source: json
        // TODO: support also CBOR and SMILE with an additional formatting parameter
        buildEntry(new BlockConverter.TransformedBytesRef("_source", MinorType.VARCHAR, ValueConversions::sourceToJson))
    );

    private static Map.Entry<String, BlockConverter> buildEntry(BlockConverter converter) {
        return Map.entry(converter.esqlType(), converter);
    }
}
