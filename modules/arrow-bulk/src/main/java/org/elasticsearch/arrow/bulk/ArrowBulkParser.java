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
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.escf.EscfBatch;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

/**
 * Parses an Arrow IPC stream and turns every record batch into bulk requests carrying a pre-built {@link EscfBatch}, so that
 * documents are indexed through the columnar indexing path without being converted to JSON.
 * <p>
 * Bulk requests can carry only one pre-built batch, so a record batch becomes one bulk request, or several if its rows target
 * different indices (see {@link #INDEX}). Record batches are never merged, which keeps memory usage proportional to the size
 * of a record batch rather than to the size of the whole stream.
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
     * Parameters of the bulk requests that are created.
     *
     * @param defaultIndex the target of rows that don't have an {@code _index} value
     * @param routing the routing of all documents
     * @param opType {@code index} or {@code create}
     * @param requireAlias whether the target must be an alias
     * @param requireDataStream whether the target must be a data stream
     * @param timeout the timeout of bulk requests
     * @param waitForActiveShards the number of active shard copies required to proceed, or {@code null} for the default
     * @param refresh the refresh policy of bulk requests, or {@code null} for the default
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
        @Nullable String refresh,
        Set<String> paramsUsed
    ) {}

    /**
     * A bulk request, and the number of the Arrow row that each of its items comes from (counted from the start of the
     * stream), to report failures in terms of what the client sent.
     */
    record PendingBulk(BulkRequest request, long[] rowNumbers) {}

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
        return arrowParser.parse(data, lastData);
    }

    @Override
    public void close() {
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

        for (var entry : rowsByIndex.entrySet()) {
            String index = entry.getKey();
            int[] rows = entry.getValue(); // null if all the rows go the same index
            int count = rows == null ? rowCount : rows.length;

            EscfBatch batch = converter.convert(schemaRoot, dictionaries, valueColumns, rows);
            final BulkRequest bulk = newBulkRequest();
            final long[] rowNumbers = new long[count];
            try {
                for (int i = 0; i < count; i++) {
                    int row = rows == null ? i : rows[i];
                    rowNumbers[i] = firstRow + row;
                    IndexRequest request = new IndexRequest(index).opType(params.opType())
                        .routing(params.routing())
                        .setRequireAlias(params.requireAlias())
                        .setRequireDataStream(params.requireDataStream())
                        .setIncludeSourceOnError(false);
                    String id = idColumn < 0 ? null : converter.stringValue(schemaRoot.getVector(idColumn), row, dictionaries);
                    if (id != null) {
                        request.id(id);
                    }
                    request.indexSource().setSourceRow(batch, i, XContentType.JSON);
                    bulk.add(request);
                }
                // The bulk request owns the batch from here on
                bulk.setPreBuiltBatches(Map.of(index, batch));
            } catch (RuntimeException e) {
                batch.close();
                throw e;
            }
            bulkConsumer.accept(new PendingBulk(bulk, rowNumbers));
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
        if (params.refresh() != null) {
            bulk.setRefreshPolicy(params.refresh());
        }
        bulk.requestParamsUsed(params.paramsUsed());
        return bulk;
    }
}
