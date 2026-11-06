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
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.arrow.Arrow;
import org.elasticsearch.arrow.ArrowFormatException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.escf.EscfBatch;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

/**
 * Parses an Arrow IPC stream and turns every record batch into bulk requests carrying a pre-built {@link EscfBatch}, so that
 * documents are indexed through the columnar indexing path without being converted to JSON.
 * <p>
 * Bulk requests can carry only one pre-built batch, so there is one bulk request per target index (see {@link #INDEX}). Every bulk
 * request has a fixed cost (it waits for the translog to be synced, on every shard that it writes to), so small record batches are
 * not sent one by one: their rows are accumulated, per target, and sent when there are {@link Params#bulkRows()} rows, when the data
 * that was accumulated for all the targets reaches {@link Params#bulkBytes()}, or when the stream ends. A record batch is never split, so a
 * record batch that is bigger than this is sent as is. Memory usage is bounded by the target size, and not by the size of the stream.
 * <p>
 * The columns of the Arrow table are the fields of the documents, except for the following ones that carry bulk metadata
 * and are not part of the documents:
 * <ul>
 * <li>{@link #ID}: the document id. If absent, ids are generated.
 * <li>{@link #INDEX}: the target index, data stream or alias. If absent, the index of the request path is used.
 * </ul>
 */
final class ArrowBulkParser implements Releasable {

    static final String ID = "_id";
    static final String INDEX = "_index";
    /** Bulk actions other than index and create aren't supported, but the column is reserved. */
    static final String ACTION = "_bulk_action";

    /**
     * The default number of rows after which the rows accumulated for a target are sent ({@code bulk_rows}). With the default
     * translog durability, a bulk request waits for the translog to be synced, so the throughput of small bulk requests is limited by
     * that: in a benchmark of 12 columns, record batches of 1000 rows were indexed at about 73k rows/s, 10k rows at 259k, and 50k rows
     * at 322k. Above 10k rows, the cost of the rows dominates.
     */
    static final int DEFAULT_BULK_ROWS = 10_000;

    /**
     * The default (approximate, it's the size of the Arrow buffers) number of bytes after which everything that was accumulated is
     * sent ({@code bulk_bytes}), to bound the memory used by the rows that wait to be sent, also when they go to many different targets.
     */
    static final long DEFAULT_BULK_BYTES = 8L * 1024 * 1024;

    /** The highest {@code bulk_bytes}: it bounds the memory that a request can use for rows that wait to be sent. */
    static final long MAX_BULK_BYTES = 64L * 1024 * 1024;

    /**
     * Parameters of the bulk requests that are created.
     *
     * @param defaultIndex the target of rows that don't have an {@code _index} value
     * @param routing the routing of all documents
     * @param opType {@code index} or {@code create}
     * @param requireAlias whether the target must be an alias
     * @param requireDataStream whether the target must be a data stream
     * @param timeout the timeout of bulk requests
     * @param waitForActiveShards the number of active shard copies required to proceed, or {@code null} for the default
     * @param bulkRows the number of rows of a target after which they are sent as a bulk request
     * @param bulkBytes the size (of the Arrow buffers) of everything that waits, after which it is all sent
     * @param paramsUsed the REST request parameters, for telemetry
     */
    record Params(
        @Nullable String defaultIndex,
        @Nullable String routing,
        DocWriteRequest.OpType opType,
        boolean requireAlias,
        boolean requireDataStream,
        TimeValue timeout,
        @Nullable ActiveShardCount waitForActiveShards,
        int bulkRows,
        long bulkBytes,
        Set<String> paramsUsed
    ) {}

    /**
     * A bulk request, the index (or alias, or data stream) it targets, and the number of the Arrow row that each of its items comes
     * from (counted from the start of the stream), to report failures in terms of what the client sent.
     * <p>
     * Bulk requests don't have a refresh policy: the refresh requested for the stream is done once, when all the bulk requests have run.
     */
    record PendingBulk(BulkRequest request, String index, long[] rowNumbers) {}

    private final Params params;
    private final BooleanSupplier batchIndexingSupported;
    private final Consumer<PendingBulk> bulkConsumer;
    private final ArrowToEscf converter;

    private final BufferAllocator allocator;
    private final ArrowIncrementalParser arrowParser;
    private VectorSchemaRoot schemaRoot;

    private int idColumn = -1;
    private int indexColumn = -1;
    private BitSet valueColumns;
    private long rowsSoFar = 0;

    /** The rows that wait to be sent, by target index. */
    private final Map<String, Accumulator> accumulators = new LinkedHashMap<>();

    /**
     * @param batchIndexingSupported whether columnar indexing can be used by the cluster
     * @param bulkConsumer receives the bulk requests, in stream order. It becomes responsible for closing them.
     */
    ArrowBulkParser(
        Params params,
        Recycler<BytesRef> recycler,
        BooleanSupplier batchIndexingSupported,
        Consumer<PendingBulk> bulkConsumer
    ) {
        this.params = params;
        this.batchIndexingSupported = batchIndexingSupported;
        this.bulkConsumer = bulkConsumer;
        this.converter = new ArrowToEscf(recycler);

        // FIXME: hard-coded limit to 100 MiB per record batch. Should we add an AllocationListener that calls ES memory management?
        this.allocator = Arrow.newChildAllocator("bulk-ingestion", 0, 100 * 1024 * 1024);
        this.arrowParser = new ArrowIncrementalParser(allocator, new ArrowIncrementalParser.Listener() {
            @Override
            public void startStream(VectorSchemaRoot root) {
                startArrowStream(root);
            }

            @Override
            public void nextBatch(Map<Long, Dictionary> dictionaries) {
                nextArrowBatch(dictionaries);
            }

            @Override
            public void endStream() {}
        });
    }

    /**
     * Parses as many complete Arrow messages as {@code data} contains, and returns the number of bytes consumed.
     *
     * @param lastData whether this is the end of the stream
     */
    int parse(BytesReference data, boolean lastData) throws IOException {
        int consumed = arrowParser.parse(data, lastData);
        if (lastData) {
            // The end of the stream: nothing will complete the rows that wait to be sent. A stream with an incomplete message fails
            // in the parser above, and these rows are dropped with the request.
            for (Accumulator accumulator : new ArrayList<>(accumulators.values())) {
                flush(accumulator);
            }
        }
        return consumed;
    }

    @Override
    public void close() {
        Releasables.close(accumulators.values());
        accumulators.clear();
        try {
            arrowParser.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        if (schemaRoot != null) {
            schemaRoot.close();
            schemaRoot = null;
        }
        allocator.close();
    }

    private void startArrowStream(VectorSchemaRoot root) {
        this.schemaRoot = root;
        var vectors = root.getFieldVectors();
        var valueColumns = new BitSet(vectors.size());
        for (int i = 0; i < vectors.size(); i++) {
            switch (vectors.get(i).getName()) {
                case ID -> idColumn = i;
                case INDEX -> indexColumn = i;
                case ACTION -> throw new ArrowFormatException(
                    "Column [" + ACTION + "] isn't supported: only index and create operations can be sent in the Arrow format"
                );
                default -> valueColumns.set(i);
            }
        }
        if (valueColumns.isEmpty()) {
            throw new ArrowFormatException("The Arrow stream has no column that can be indexed");
        }
        this.valueColumns = valueColumns;
    }

    private void nextArrowBatch(Map<Long, Dictionary> dictionaries) {
        int rowCount = schemaRoot.getRowCount();
        if (rowCount == 0) {
            return;
        }
        if (batchIndexingSupported.getAsBoolean() == false) {
            throw new IllegalArgumentException(
                "Arrow bulk ingestion requires batch indexing, which isn't available "
                    + "(the setting is disabled, or the cluster contains nodes that don't support it)"
            );
        }

        // Group rows by target index. Only one pre-built batch per bulk request is supported.
        Map<String, int[]> rowsByIndex = groupRowsByIndex(rowCount, dictionaries);
        long firstRow = rowsSoFar;
        rowsSoFar += rowCount;

        long recordBatchBytes = 0;
        for (int column = valueColumns.nextSetBit(0); column >= 0; column = valueColumns.nextSetBit(column + 1)) {
            recordBatchBytes += schemaRoot.getVector(column).getBufferSize();
        }

        for (var entry : rowsByIndex.entrySet()) {
            int[] rows = entry.getValue(); // null if all the rows go the same index
            int count = rows == null ? rowCount : rows.length;
            Accumulator accumulator = accumulators.computeIfAbsent(entry.getKey(), Accumulator::new);
            accumulator.append(dictionaries, rows, count, firstRow, recordBatchBytes * count / rowCount);
        }

        // Everything is sent when too much waits, otherwise only the targets that have enough rows
        long accumulatedBytes = 0;
        for (Accumulator accumulator : accumulators.values()) {
            accumulatedBytes += accumulator.bytes;
        }
        for (Accumulator accumulator : new ArrayList<>(accumulators.values())) {
            if (accumulatedBytes >= params.bulkBytes() || accumulator.rows() >= params.bulkRows()) {
                flush(accumulator);
            }
        }
    }

    /** Sends the rows that were accumulated for a target, as a bulk request with a single pre-built batch. */
    private void flush(Accumulator accumulator) {
        accumulators.remove(accumulator.index);
        int rows = accumulator.rows();
        // The batch owns the columns from here on, there is nothing left to release in the accumulator
        EscfBatch batch = accumulator.conversion.build();
        final BulkRequest bulk = newBulkRequest();
        try {
            for (int i = 0; i < rows; i++) {
                IndexRequest request = new IndexRequest(accumulator.index).opType(params.opType())
                    .routing(params.routing())
                    .setRequireAlias(params.requireAlias())
                    .setRequireDataStream(params.requireDataStream())
                    .setIncludeSourceOnError(false);
                String id = accumulator.ids == null ? null : accumulator.ids.get(i);
                if (id != null) {
                    request.id(id);
                }
                request.indexSource().setSourceRow(batch, i, XContentType.JSON);
                bulk.add(request);
            }
            // The bulk request owns the batch from here on
            bulk.setPreBuiltBatches(Map.of(accumulator.index, batch));
        } catch (RuntimeException e) {
            batch.close();
            throw e;
        }
        bulkConsumer.accept(new PendingBulk(bulk, accumulator.index, Arrays.copyOf(accumulator.rowNumbers, rows)));
    }

    /** The rows going to a target index, accumulated from several record batches until they are sent. */
    private final class Accumulator implements Releasable {
        final String index;
        final ArrowToEscf.Conversion conversion = converter.newConversion();
        /** The number of the row in the stream, for each row. */
        long[] rowNumbers = new long[256];
        /** The id of each row, or {@code null} if there is no {@code _id} column. */
        final List<String> ids;
        /** The approximate size of what was accumulated, in bytes. */
        long bytes;

        Accumulator(String index) {
            this.index = index;
            this.ids = idColumn < 0 ? null : new ArrayList<>();
        }

        int rows() {
            return conversion.rowCount();
        }

        /**
         * Appends the rows of the current record batch.
         *
         * @param rows the rows of the record batch, or {@code null} for all of them
         * @param firstRow the number in the stream of the first row of the record batch
         * @param appendedBytes the share of the size of the record batch that is for these rows
         */
        void append(Map<Long, Dictionary> dictionaries, @Nullable int[] rows, int count, long firstRow, long appendedBytes) {
            int before = rows();
            rowNumbers = ArrayUtil.grow(rowNumbers, before + count);
            for (int i = 0; i < count; i++) {
                int row = rows == null ? i : rows[i];
                rowNumbers[before + i] = firstRow + row;
                if (ids != null) {
                    ids.add(converter.stringValue(schemaRoot.getVector(idColumn), row, dictionaries));
                }
            }
            conversion.append(schemaRoot, dictionaries, valueColumns, rows);
            bytes += appendedBytes;
        }

        @Override
        public void close() {
            conversion.close();
        }
    }

    /**
     * Returns the rows of the current record batch for each target index. The rows are {@code null} when all the rows of the
     * record batch go to the same index, which is the common case.
     */
    private Map<String, int[]> groupRowsByIndex(int rowCount, Map<Long, Dictionary> dictionaries) {
        FieldVector indexVector = indexColumn < 0 ? null : schemaRoot.getVector(indexColumn);
        if (indexVector == null) {
            return singleIndex(defaultIndex());
        }

        String[] indices = new String[rowCount];
        boolean allSame = true;
        for (int row = 0; row < rowCount; row++) {
            String index = converter.stringValue(indexVector, row, dictionaries);
            indices[row] = index == null ? defaultIndex() : index;
            allSame &= indices[row].equals(indices[0]);
        }
        if (allSame) {
            return singleIndex(indices[0]);
        }

        Map<String, int[]> result = new LinkedHashMap<>();
        Map<String, Integer> counts = new LinkedHashMap<>();
        for (String index : indices) {
            counts.merge(index, 1, Integer::sum);
        }
        Map<String, Integer> fill = new LinkedHashMap<>();
        for (var entry : counts.entrySet()) {
            result.put(entry.getKey(), new int[entry.getValue()]);
            fill.put(entry.getKey(), 0);
        }
        for (int row = 0; row < rowCount; row++) {
            int slot = fill.merge(indices[row], 1, Integer::sum) - 1;
            result.get(indices[row])[slot] = row;
        }
        return result;
    }

    private static Map<String, int[]> singleIndex(String index) {
        Map<String, int[]> result = new LinkedHashMap<>(1);
        result.put(index, null);
        return result;
    }

    private String defaultIndex() {
        if (params.defaultIndex() == null) {
            throw new IllegalArgumentException("No target index: set it in the request path or in an [" + INDEX + "] column");
        }
        return params.defaultIndex();
    }

    private BulkRequest newBulkRequest() {
        BulkRequest bulk = new BulkRequest();
        bulk.timeout(params.timeout());
        if (params.waitForActiveShards() != null) {
            bulk.waitForActiveShards(params.waitForActiveShards());
        }
        bulk.requestParamsUsed(params.paramsUsed());
        return bulk;
    }
}
