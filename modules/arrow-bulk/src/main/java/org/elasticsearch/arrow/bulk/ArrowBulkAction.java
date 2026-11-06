/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.arrow.bulk;

import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BatchIndexingEnabled;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkShardRequest;
import org.elasticsearch.action.bulk.ShardBatchIndexer;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.arrow.Arrow;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.transport.Transports;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

/**
 * Bulk ingestion of Arrow IPC streams.
 * <p>
 * Every Arrow record batch is converted to a columnar batch and indexed as a bulk request. Requests are executed one at a time
 * so that a streamed request only asks for the next HTTP chunk once the previous record batch was indexed: this applies
 * back-pressure to the client and bounds memory usage.
 * <p>
 * The response is an Arrow stream that describes the failed items. It is an empty table if all items were indexed. Failures that
 * affect the whole request (for example a malformed stream) are returned as regular JSON errors, and items that come after
 * the failure aren't indexed.
 *
 * @see ArrowBulkParser for the structure of the data
 */
public class ArrowBulkAction extends BaseRestHandler {

    public static final String ID = ArrowBulkParser.ID;
    public static final String INDEX = ArrowBulkParser.INDEX;

    public static final String ERR_ITEM_NO = "item_no";
    public static final String ERR_STATUS = "status";
    public static final String ERR_TYPE = "type";
    public static final String ERR_REASON = "reason";

    private final ClusterService clusterService;
    private final BatchIndexingEnabled batchIndexingEnabled;
    private final Recycler<BytesRef> recycler;

    public ArrowBulkAction(ClusterService clusterService, Recycler<BytesRef> recycler) {
        this.clusterService = clusterService;
        this.batchIndexingEnabled = new BatchIndexingEnabled(clusterService.getClusterSettings());
        this.recycler = recycler;
    }

    @Override
    public String getName() {
        return "arrow_bulk_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(POST, "/_arrow/_bulk"),
            new Route(PUT, "/_arrow/_bulk"),
            new Route(POST, "/_arrow/{index}/_bulk"),
            new Route(PUT, "/_arrow/{index}/_bulk")
        );
    }

    @Override
    public boolean mediaTypesValid(RestRequest request) {
        return request.getParsedContentType() != null
            && Arrow.MEDIA_TYPE.equals(request.getParsedContentType().mediaTypeWithoutParameters());
    }

    @Override
    public boolean supportsContentStream() {
        return true;
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        if (request.param("pipeline") != null) {
            throw new IllegalArgumentException("Ingest pipelines are not supported with Arrow bulk ingestion");
        }
        if (request.hasParam("op_type")
            && "index".equals(request.param("op_type")) == false
            && "create".equals(request.param("op_type")) == false) {
            throw new IllegalArgumentException("[op_type] must be [index] or [create]");
        }

        // We default to create requests as it's safe and versatile:
        // - accepts requests with and without an id,
        // - if an id is present, ensures we don't accidentally overwrite an existing document,
        // - datastreams only accept create operations.
        var opType = DocWriteRequest.OpType.fromString(request.param("op_type", DocWriteRequest.OpType.CREATE.getLowercase()));
        String waitForActiveShards = request.param("wait_for_active_shards");

        // The rows of record batches are sent in bulk requests of about this size
        int bulkRows = request.paramAsInt("bulk_rows", ArrowBulkParser.DEFAULT_BULK_ROWS);
        if (bulkRows < 1) {
            throw new IllegalArgumentException("[bulk_rows] must be greater than zero, but was [" + bulkRows + "]");
        }
        ByteSizeValue bulkBytes = request.paramAsSize("bulk_bytes", ByteSizeValue.ofBytes(ArrowBulkParser.DEFAULT_BULK_BYTES));
        if (bulkBytes.getBytes() < 1 || bulkBytes.getBytes() > ArrowBulkParser.MAX_BULK_BYTES) {
            throw new IllegalArgumentException(
                "[bulk_bytes] must be between [1b] and ["
                    + ByteSizeValue.ofBytes(ArrowBulkParser.MAX_BULK_BYTES)
                    + "], but was ["
                    + bulkBytes
                    + "]"
            );
        }

        var params = new ArrowBulkParser.Params(
            request.param("index"),
            request.param("routing"),
            opType,
            request.paramAsBoolean(DocWriteRequest.REQUIRE_ALIAS, false),
            request.paramAsBoolean(DocWriteRequest.REQUIRE_DATA_STREAM, false),
            request.paramAsTime("timeout", BulkShardRequest.DEFAULT_TIMEOUT),
            waitForActiveShards == null ? null : ActiveShardCount.parseString(waitForActiveShards),
            bulkRows,
            bulkBytes.getBytes(),
            request.params().keySet()
        );

        // Refreshing after every bulk request of a stream is very costly (with wait_for, each of them waits for a refresh), so the
        // refresh is done once, at the end. It's validated now, to reject the request before reading any data.
        String refresh = request.param("refresh");
        WriteRequest.RefreshPolicy refreshPolicy = refresh == null
            ? WriteRequest.RefreshPolicy.NONE
            : WriteRequest.RefreshPolicy.parse(refresh);
        return new ChunkHandler(request, client, params, refreshPolicy);
    }

    /**
     * Consumes the body of the request, as a stream or as a single chunk, and runs the resulting bulk requests.
     */
    private class ChunkHandler implements BaseRestHandler.RequestBodyChunkConsumer {

        private final RestRequest request;
        private final NodeClient client;
        private final ArrowBulkParser parser;

        /**
         * Received HTTP chunks that aren't fully parsed yet. An Arrow message can span several chunks, so bytes are kept until a
         * complete message is available. Chunks are released as the parser consumes them, and the unconsumed tail is kept.
         */
        private final ArrayDeque<ReleasableBytesReference> unparsedChunks = new ArrayDeque<>();

        /**
         * Bulk requests created from the parsed record batches, that haven't been sent yet. They are executed one at a time, and the next
         * chunk is requested only once the queue is empty, which applies back-pressure to the client.
         */
        private final ArrayDeque<ArrowBulkParser.PendingBulk> pendingBulks = new ArrayDeque<>();

        /** Whether to refresh the written indices once all bulk requests have run, to make the documents visible to searches. */
        private final WriteRequest.RefreshPolicy refreshPolicy;

        /**
         * The indices that were written to, to refresh. An explicit refresh of the indices covers all their shards, which the last bulk
         * request with a refresh policy wouldn't: it may not have written to every shard that earlier bulk requests wrote to.
         */
        private final Set<String> writtenIndices = new LinkedHashSet<>();

        /** Items that failed, collected from the bulk responses to build the Arrow response. */
        private final List<ItemFailure> failures = new ArrayList<>();

        private volatile RestChannel channel;
        private boolean shortCircuited;
        private boolean lastChunk;

        ChunkHandler(RestRequest request, NodeClient client, ArrowBulkParser.Params params, WriteRequest.RefreshPolicy refreshPolicy) {
            this.request = request;
            this.client = client;
            this.refreshPolicy = refreshPolicy;
            this.parser = new ArrowBulkParser(
                params,
                recycler,
                () -> ShardBatchIndexer.isBatchIndexingSupported(batchIndexingEnabled, clusterService),
                pendingBulks::add
            );
        }

        @Override
        public void accept(RestChannel channel) {
            this.channel = channel;
            if (request.isStreamedContent()) {
                request.contentStream().next();
            } else {
                // The whole body is available.
                ReleasableBytesReference content = request.content();
                content.mustIncRef();
                handleChunk(channel, content, true);
            }
        }

        @Override
        public void handleChunk(RestChannel channel, ReleasableBytesReference chunk, boolean isLast) {
            assert channel == this.channel;
            if (shortCircuited) {
                chunk.close();
                return;
            }

            try {
                if (chunk.length() == 0) {
                    chunk.close();
                } else {
                    unparsedChunks.add(chunk);
                }
                // The last chunk can be empty. The parser must still be told that the stream ended, to detect that it was truncated.
                if (unparsedChunks.isEmpty() == false || isLast) {
                    BytesReference data = switch (unparsedChunks.size()) {
                        case 0 -> BytesArray.EMPTY;
                        case 1 -> unparsedChunks.getFirst();
                        default -> CompositeBytesReference.of(unparsedChunks.toArray(new ReleasableBytesReference[0]));
                    };
                    // Record batches are converted to bulk requests (added to pendingBulks) as they are parsed
                    releaseParsed(parser.parse(data, isLast));
                }
            } catch (Exception e) {
                fail(e);
                return;
            }

            this.lastChunk = isLast;
            executeNext();
        }

        /** Executes pending bulk requests one at a time, then asks for more data or sends the response. */
        private void executeNext() {
            ArrowBulkParser.PendingBulk next = pendingBulks.poll();
            if (next == null) {
                if (lastChunk) {
                    refreshThenSendResponse();
                } else {
                    request.contentStream().next();
                }
                return;
            }

            writtenIndices.add(next.index());
            // Hands the batch over to the bulk action, that releases it.
            client.bulk(next.request(), new ActionListener<>() {
                @Override
                public void onResponse(org.elasticsearch.action.bulk.BulkResponse response) {
                    for (BulkItemResponse item : response.getItems()) {
                        if (item.isFailed()) {
                            failures.add(ItemFailure.of(next.rowNumbers()[item.getItemId()], item.getFailure()));
                        }
                    }
                    executeNext();
                }

                @Override
                public void onFailure(Exception e) {
                    fail(e);
                }
            });
        }

        /** Refreshes the indices that were written to if the request asked for it, and then sends the response. */
        private void refreshThenSendResponse() {
            if (refreshPolicy == WriteRequest.RefreshPolicy.NONE || writtenIndices.isEmpty()) {
                sendResponse();
                return;
            }
            var refresh = new RefreshRequest(writtenIndices.toArray(String[]::new));
            client.admin().indices().refresh(refresh, ActionListener.wrap(response -> sendResponse(), this::fail));
        }

        private void sendResponse() {
            shortCircuit();
            try {
                channel.sendResponse(new RestResponse(RestStatus.OK, Arrow.MEDIA_TYPE, failuresToArrow(failures)));
            } catch (Exception e) {
                new RestToXContentListener<>(channel).onFailure(e);
            }
        }

        private void fail(Exception e) {
            shortCircuit();
            Exception cause = e instanceof IllegalArgumentException || e instanceof ElasticsearchException
                ? e
                : new ElasticsearchParseException("Failed to parse Arrow format", e);
            new RestToXContentListener<>(channel).onFailure(cause);
        }

        @Override
        public void streamClose() {
            assert Transports.assertTransportThread();
            if (shortCircuited == false) {
                shortCircuit();
            }
        }

        private void shortCircuit() {
            shortCircuited = true;
            Releasables.closeExpectNoException(parser);
            for (var pending : pendingBulks) {
                pending.request().releasePreBuiltBatches();
            }
            pendingBulks.clear();
            Releasables.close(unparsedChunks);
            unparsedChunks.clear();
        }

        /** Releases chunks that were fully parsed, and keeps the unparsed tail of the last one. */
        private void releaseParsed(int bytesConsumed) {
            List<Releasable> parsed = new ArrayList<>(unparsedChunks.size());
            while (bytesConsumed > 0) {
                ReleasableBytesReference reference = unparsedChunks.removeFirst();
                parsed.add(reference);
                if (bytesConsumed >= reference.length()) {
                    bytesConsumed -= reference.length();
                } else {
                    unparsedChunks.addFirst(reference.retainedSlice(bytesConsumed, reference.length() - bytesConsumed));
                    bytesConsumed = 0;
                }
            }
            Releasables.close(parsed);
        }
    }

    private record ItemFailure(long itemNo, String index, String id, int status, String type, String reason) {
        static ItemFailure of(long itemNo, BulkItemResponse.Failure failure) {
            Throwable cause = ExceptionsHelper.unwrapCause(failure.getCause());
            return new ItemFailure(
                itemNo,
                failure.getIndex(),
                failure.getId(),
                failure.getStatus().getStatus(),
                cause == null ? null : ElasticsearchException.getExceptionName(cause),
                cause == null ? null : cause.getMessage()
            );
        }
    }

    private static BytesReference failuresToArrow(List<ItemFailure> failures) throws IOException {
        var output = new BytesReferenceOutputStream();
        try (
            var allocator = Arrow.newChildAllocator("bulk_response", 0, 10_000_000L);
            var itemNoVector = new UInt4Vector(ERR_ITEM_NO, allocator);
            var indexVector = new VarCharVector(INDEX, allocator);
            var idVector = new VarCharVector(ID, allocator);
            var statusVector = new UInt2Vector(ERR_STATUS, allocator);
            var typeVector = new VarCharVector(ERR_TYPE, allocator);
            var reasonVector = new VarCharVector(ERR_REASON, allocator);
            var root = new VectorSchemaRoot(List.of(itemNoVector, indexVector, idVector, statusVector, typeVector, reasonVector));
            var writer = new ArrowStreamWriter(root, null, output)
        ) {
            for (int i = 0; i < failures.size(); i++) {
                ItemFailure failure = failures.get(i);
                itemNoVector.setSafe(i, Math.toIntExact(failure.itemNo()));
                setString(indexVector, i, failure.index());
                setString(idVector, i, failure.id());
                statusVector.setSafe(i, failure.status());
                setString(typeVector, i, failure.type());
                setString(reasonVector, i, failure.reason());
            }
            for (var vector : root.getFieldVectors()) {
                vector.setValueCount(failures.size());
            }
            root.setRowCount(failures.size());
            writer.writeBatch();
        }
        return output.asBytesReference();
    }

    private static void setString(VarCharVector vector, int position, String value) {
        if (value == null) {
            vector.setNull(position);
        } else {
            vector.setSafe(position, value.getBytes(StandardCharsets.UTF_8));
        }
    }
}
