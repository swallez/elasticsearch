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
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

/**
 * Bulk ingestion of Arrow IPC streams.
 * <p>
 * The rows of the Arrow record batches are converted to columnar batches and indexed as bulk requests (see
 * {@link ArrowBulkParser}). Up to {@link #MAX_IN_FLIGHT_BULKS} bulk requests of a stream run at the same time if the stream has no
 * document ids: the documents are then independent, and parsing the next data overlaps with indexing. With ids, bulk requests run one
 * at a time, in the order of the stream, as documents can overwrite each other. The next chunk of a streamed request is only
 * requested when no bulk request waits to be sent, which applies back-pressure to the client and bounds memory usage.
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

    /** The number of bulk requests of a stream that can run at the same time, when the order in which they run doesn't matter. */
    static final int MAX_IN_FLIGHT_BULKS = 4;

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
     * <p>
     * Chunks are received by the thread of the HTTP channel, and the bulk requests complete on other threads. All the state is
     * guarded by {@link #lock}, which is never held while calling something that can call back: sending a bulk request, requesting
     * a chunk, or sending the response.
     */
    private class ChunkHandler implements BaseRestHandler.RequestBodyChunkConsumer {

        private final RestRequest request;
        private final NodeClient client;
        private final ArrowBulkParser parser;
        private final Object lock = new Object();

        /**
         * Received HTTP chunks that aren't fully parsed yet. An Arrow message can span several chunks, so bytes are kept until a
         * complete message is available. Chunks are released as the parser consumes them, and the unconsumed tail is kept.
         */
        private final ArrayDeque<ReleasableBytesReference> unparsedChunks = new ArrayDeque<>();

        /**
         * Decides when to send the bulk requests that are created from the parsed record batches, and when to request more data. It
         * holds the bulk requests that were not sent yet. Bulk requests are executed at the same time, but only when the order in
         * which they are executed doesn't matter, so that parsing the next data overlaps with indexing.
         */
        private final BulkPipeline<ArrowBulkParser.PendingBulk> pipeline = new BulkPipeline<>(this::maxInFlight);

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

        /** Whether the request is finished, because the response was sent, or because it failed or the client is gone. */
        private boolean closed;

        ChunkHandler(RestRequest request, NodeClient client, ArrowBulkParser.Params params, WriteRequest.RefreshPolicy refreshPolicy) {
            this.request = request;
            this.client = client;
            this.refreshPolicy = refreshPolicy;
            this.parser = new ArrowBulkParser(
                params,
                recycler,
                () -> ShardBatchIndexer.isBatchIndexingSupported(batchIndexingEnabled, clusterService),
                pipeline::add
            );
        }

        /**
         * The number of bulk requests that can run at the same time. Documents with an id can overwrite each other, or conflict, so
         * their bulk requests must run one at a time to keep the order of the stream. Without ids, the documents are independent.
         */
        private int maxInFlight() {
            return parser.hasIdColumn() ? 1 : MAX_IN_FLIGHT_BULKS;
        }

        @Override
        public void accept(RestChannel channel) {
            this.channel = channel;
            if (request.isStreamedContent()) {
                step();
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
            Exception failure = null;
            synchronized (lock) {
                if (closed) {
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
                        // Record batches are converted to bulk requests (added to the pipeline) as they are parsed
                        releaseParsed(parser.parse(data, isLast));
                    }
                    pipeline.chunkReceived(isLast);
                } catch (Exception e) {
                    failure = e;
                }
            }
            if (failure != null) {
                fail(failure);
            } else {
                step();
            }
        }

        /** Does what the pipeline decides: sends the bulk requests that can run, requests more data, or finishes. */
        private void step() {
            List<BulkPipeline.Step<ArrowBulkParser.PendingBulk>> steps = new ArrayList<>(MAX_IN_FLIGHT_BULKS + 1);
            synchronized (lock) {
                if (closed) {
                    return;
                }
                BulkPipeline.Step<ArrowBulkParser.PendingBulk> step;
                while ((step = pipeline.next()) != null) {
                    if (step.kind() == BulkPipeline.Kind.SEND) {
                        writtenIndices.add(step.bulk().index());
                    }
                    steps.add(step);
                }
            }
            for (int i = 0; i < steps.size(); i++) {
                try {
                    var step = steps.get(i);
                    switch (step.kind()) {
                        case SEND -> send(step.bulk());
                        case REQUEST_CHUNK -> request.contentStream().next();
                        case FINISH -> refreshThenSendResponse();
                    }
                } catch (Exception e) {
                    // The bulk requests that weren't sent still own their batches
                    for (int j = i; j < steps.size(); j++) {
                        if (steps.get(j).kind() == BulkPipeline.Kind.SEND) {
                            steps.get(j).bulk().request().releasePreBuiltBatches();
                        }
                    }
                    fail(e);
                    return;
                }
            }
        }

        private void send(ArrowBulkParser.PendingBulk bulk) {
            // Hands the batch over to the bulk action, that releases it.
            client.bulk(bulk.request(), new ActionListener<>() {
                @Override
                public void onResponse(org.elasticsearch.action.bulk.BulkResponse response) {
                    synchronized (lock) {
                        pipeline.bulkCompleted();
                        if (closed == false) {
                            for (BulkItemResponse item : response.getItems()) {
                                if (item.isFailed()) {
                                    failures.add(ItemFailure.of(bulk.rowNumbers()[item.getItemId()], item.getFailure()));
                                }
                            }
                        }
                    }
                    step();
                }

                @Override
                public void onFailure(Exception e) {
                    fail(e);
                }
            });
        }

        /** Refreshes the indices that were written to if the request asked for it, and then sends the response. */
        private void refreshThenSendResponse() {
            final String[] indices;
            synchronized (lock) {
                if (closed) {
                    return;
                }
                indices = refreshPolicy == WriteRequest.RefreshPolicy.NONE ? new String[0] : writtenIndices.toArray(String[]::new);
            }
            if (indices.length == 0) {
                sendResponse();
                return;
            }
            client.admin().indices().refresh(new RefreshRequest(indices), ActionListener.wrap(response -> sendResponse(), this::fail));
        }

        private void sendResponse() {
            final List<ItemFailure> sorted;
            synchronized (lock) {
                if (closed) {
                    return;
                }
                // Bulk requests can complete in any order
                sorted = new ArrayList<>(failures);
                sorted.sort(Comparator.comparingLong(ItemFailure::itemNo));
                shortCircuit();
            }
            try {
                channel.sendResponse(new RestResponse(RestStatus.OK, Arrow.MEDIA_TYPE, failuresToArrow(sorted)));
            } catch (Exception e) {
                new RestToXContentListener<>(channel).onFailure(e);
            }
        }

        private void fail(Exception e) {
            synchronized (lock) {
                if (closed) {
                    return;
                }
                shortCircuit();
            }
            Exception cause = e instanceof IllegalArgumentException || e instanceof ElasticsearchException
                ? e
                : new ElasticsearchParseException("Failed to parse Arrow format", e);
            new RestToXContentListener<>(channel).onFailure(cause);
        }

        @Override
        public void streamClose() {
            assert Transports.assertTransportThread();
            synchronized (lock) {
                if (closed == false) {
                    shortCircuit();
                }
            }
        }

        /** Closes the request and releases what it holds. The bulk requests that are running own their batches. */
        private void shortCircuit() {
            assert Thread.holdsLock(lock);
            closed = true;
            Releasables.closeExpectNoException(parser);
            for (var pending : pipeline.drainPending()) {
                pending.request().releasePreBuiltBatches();
            }
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
