/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.arrow.bulk;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.apache.http.entity.AbstractHttpEntity;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.InputStreamEntity;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.bulk.BatchIndexingEnabled;
import org.elasticsearch.action.bulk.ShardBatchIndexer;
import org.elasticsearch.action.bulk.TransportBulkAction;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.arrow.Arrow;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.node.NodeMocksPlugin;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.junit.After;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.Socket;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * End-to-end tests for Arrow bulk ingestion: Arrow data is sent over HTTP, indexed, and then searched or fetched. Conversions of
 * the various Arrow data types are covered in {@link ArrowToEscfTests}.
 * <p>
 * Documents of a batch are indexed on the primary shard either with the columnar mapper, or, when the index can't use it (it depends
 * on the index mode), by materializing each row and indexing documents one by one. Both must give the same results, so these tests are
 * run for each of them by subclasses, that define the index mode and the path that is expected.
 * <p>
 * Rows that conflict with an existing document ({@code create} of an existing id) are reported as failures in both modes, which is
 * what is tested for the Arrow endpoint. In a columnar index in batch mode however, a conflicting row that is followed by at least one
 * successful row of the same bulk is also written to the segment, without an {@code _id} doc value: the document count is then too
 * high, and a search that fetches such a document fails an assertion. A conflicting row at the end of a bulk is not affected. This
 * isn't specific to Arrow (it happens with a JSON bulk, for example the ids {@code a, a, b}), so the document count isn't checked
 * after conflicts in that mode, see {@link #assertCountAfterConflicts}. This is tracked in
 * <a href="https://github.com/elastic/elasticsearch/issues/164362">#164362</a>.
 */
public abstract class ArrowBulkTestCase extends ESSingleNodeRestTestCase {

    protected RestClient restClient;

    @Before
    public void init() {
        restClient = createRestClient();
    }

    @After
    public void cleanup() throws IOException {
        restClient.close();
    }

    /**
     * Holds the bulk requests that are sent to the bulk action while {@link #holding} is set, until they are released, to have
     * a bulk request that is running for as long as a test needs.
     */
    public static class HoldBulksPlugin extends Plugin implements ActionPlugin {
        static volatile boolean holding = false;
        static final Queue<Runnable> held = new ConcurrentLinkedQueue<>();
        /** The number of bulk requests that were sent to the bulk action, to check how many are made from the record batches. */
        static final AtomicInteger bulkRequests = new AtomicInteger();

        /** Lets the bulk requests that are held continue. */
        static void releaseAll() {
            Runnable bulk;
            while ((bulk = held.poll()) != null) {
                bulk.run();
            }
        }

        @Override
        public Collection<ActionFilter> getActionFilters() {
            return List.of(new ActionFilter() {
                @Override
                public int order() {
                    return 0;
                }

                @Override
                public <Request extends ActionRequest, Response extends ActionResponse> void apply(
                    Task task,
                    String action,
                    Request request,
                    ActionListener<Response> listener,
                    ActionFilterChain<Request, Response> chain
                ) {
                    if (TransportBulkAction.NAME.equals(action)) {
                        bulkRequests.incrementAndGet();
                    }
                    if (holding && TransportBulkAction.NAME.equals(action)) {
                        held.add(() -> chain.proceed(task, action, request, listener));
                    } else {
                        chain.proceed(task, action, request, listener);
                    }
                }
            });
        }
    }

    @After
    public void releaseHeldBulks() {
        HoldBulksPlugin.holding = false;
        HoldBulksPlugin.releaseAll();
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        // NodeMocksPlugin makes the node use mock BigArrays and page recycler: ESTestCase then fails a test that leaks
        // pages, which is how leaks of the ESCF batches that are created for Arrow data are detected.
        return List.of(ArrowPlugin.class, DataStreamsPlugin.class, NodeMocksPlugin.class, HoldBulksPlugin.class);
    }

    @Override
    protected Settings nodeSettings() {
        return Settings.builder().put(super.nodeSettings()).put(BatchIndexingEnabled.BATCH_INDEXING.getKey(), true).build();
    }

    // ---------------------------------------------------------------------------------------------------------------------------------
    // Helpers

    /** The settings that put an index in the mode that is tested. */
    protected abstract Settings.Builder indexModeSettings();

    /** Whether the primary shards index the batches of such an index with the columnar mapper, instead of one document at a time. */
    protected abstract boolean usesColumnarPath();

    /** Arrow data is ingested through batch indexing, which requires an index that opted in. */
    protected void createBatchIndex(String index) {
        createBatchIndex(index, "ints", "type=integer", "strings", "type=keyword");
    }

    protected void createBatchIndex(String index, String... mapping) {
        createBatchIndex(index, Settings.builder(), mapping);
    }

    protected void createBatchIndex(String index, Settings.Builder settings, String... mapping) {
        assertAcked(
            indicesAdmin().prepareCreate(index)
                .setSettings(indexModeSettings().put(settings.build()).put(IndexSettings.BATCH_INDEXING.getKey(), true))
                .setMapping(mapping)
        );
    }

    protected static Field field(String name, ArrowType type, Field... children) {
        return new Field(name, FieldType.nullable(type), children.length == 0 ? null : List.of(children));
    }

    protected static Field intField(String name) {
        return field(name, new ArrowType.Int(32, true));
    }

    protected static Field stringField(String name) {
        return field(name, new ArrowType.Utf8());
    }

    /**
     * Writes an Arrow stream of {@code batches} record batches. {@code filler} must set the values and the row count of the root
     * for the given batch number.
     */
    protected static byte[] arrowStream(Schema schema, int batches, BiConsumer<VectorSchemaRoot, Integer> filler) throws IOException {
        var baos = new ByteArrayOutputStream();
        try (var allocator = Arrow.newChildAllocator("test", 0, Long.MAX_VALUE); var root = VectorSchemaRoot.create(schema, allocator)) {
            try (var writer = new ArrowStreamWriter(root, null, baos)) {
                for (int batch = 0; batch < batches; batch++) {
                    filler.accept(root, batch);
                    writer.writeBatch();
                }
            }
        }
        return baos.toByteArray();
    }

    /** A stream with a keyword {@code _id} column, and an integer {@code ints} column that is the row number. */
    protected static byte[] idsStream(String... ids) throws IOException {
        return arrowStream(new Schema(List.of(stringField(ArrowBulkAction.ID), intField("ints"))), 1, (root, batch) -> {
            for (int i = 0; i < ids.length; i++) {
                ((VarCharVector) root.getVector(0)).setSafe(i, new Text(ids[i]));
                ((IntVector) root.getVector(1)).setSafe(i, i);
            }
            root.setRowCount(ids.length);
        });
    }

    /** Sends a stream, randomly either as a single request body or as a chunked (streamed) one. */
    protected Response post(String index, byte[] payload, Map<String, String> params) throws IOException {
        return post(index, payload, params, randomBoolean());
    }

    protected Response post(String index, byte[] payload, Map<String, String> params, boolean chunked) throws IOException {
        var request = new Request("POST", index == null ? "/_arrow/_bulk" : "/_arrow/" + index + "/_bulk");
        params.forEach(request::addParameter);
        request.setOptions(request.getOptions().toBuilder().addHeader("Content-type", Arrow.MEDIA_TYPE));
        AbstractHttpEntity entity;
        if (chunked) {
            entity = new InputStreamEntity(new ByteArrayInputStream(payload), ContentType.create(Arrow.MEDIA_TYPE));
            entity.setChunked(true);
        } else {
            entity = new ByteArrayEntity(payload, ContentType.create(Arrow.MEDIA_TYPE));
        }
        request.setEntity(entity);
        return restClient.performRequest(request);
    }

    protected Response post(String index, byte[] payload) throws IOException {
        return post(index, payload, Map.of("refresh", "true"));
    }

    protected record Failure(int itemNo, int status) {}

    /** Reads the failures of a bulk response, that is an Arrow stream. */
    protected static List<Failure> failures(Response response) throws IOException {
        assertEquals(Arrow.MEDIA_TYPE, response.getHeader("Content-Type"));
        try (
            var allocator = Arrow.newChildAllocator("test", 0, Long.MAX_VALUE);
            var reader = new ArrowStreamReader(response.getEntity().getContent(), allocator)
        ) {
            assertTrue(reader.loadNextBatch());
            var root = reader.getVectorSchemaRoot();
            var itemNos = (UInt4Vector) root.getVector(ArrowBulkAction.ERR_ITEM_NO);
            var statuses = (UInt2Vector) root.getVector(ArrowBulkAction.ERR_STATUS);
            List<Failure> result = new ArrayList<>();
            for (int i = 0; i < root.getRowCount(); i++) {
                result.add(new Failure(itemNos.get(i), statuses.get(i)));
            }
            return result;
        }
    }

    protected static ResponseException expectFailure(IOExceptionRunnable request) {
        return expectThrows(ResponseException.class, request::run);
    }

    @FunctionalInterface
    protected interface IOExceptionRunnable {
        void run() throws IOException;
    }

    protected static void assertBadRequest(ResponseException e, String message) throws IOException {
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        assertThat(
            new String(e.getResponse().getEntity().getContent().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8),
            containsString(message)
        );
    }

    protected long count(String index) throws IOException {
        var response = restClient.performRequest(new Request("GET", "/" + index + "/_count"));
        var result = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, response.getEntity().getContent()).map();
        return ((Number) result.get("count")).longValue();
    }

    protected void assertCount(String index, long count) throws IOException {
        assertEquals(count, count(index));
    }

    /**
     * Asserts the number of documents of an index, after a request with rows that conflicted with existing documents.
     * <p>
     * The check is skipped for the columnar path, that also writes some of the conflicting rows, see
     * <a href="https://github.com/elastic/elasticsearch/issues/164362">#164362</a>. Remove the workaround once it is fixed, so that the
     * count is checked in both modes.
     */
    protected void assertCountAfterConflicts(String index, long expected) throws IOException {
        if (usesColumnarPath() == false) {
            assertCount(index, expected);
        }
    }

    // ---------------------------------------------------------------------------------------------------------------------------------
    // Tests

    /**
     * An end-to-end test that checks that Arrow data is correctly indexed and can be searched.
     */
    public void testBulk() throws Exception {
        String index = "arrow_bulk_test";
        createBatchIndex(index);

        int batchCount = randomIntBetween(1, 10);
        int rowCount = randomIntBetween(1, 10);

        // A dataframe with two columns: integer and string
        byte[] payload = arrowStream(new Schema(List.of(intField("ints"), stringField("strings"))), batchCount, (root, batch) -> {
            for (int row = 0; row < rowCount; row++) {
                int globalRow = row + batch * rowCount;
                ((IntVector) root.getVector(0)).setSafe(row, globalRow);
                ((VarCharVector) root.getVector(1)).setSafe(row, new Text("row" + globalRow));
            }
            root.setRowCount(rowCount);
        });

        // The response is an Arrow stream with empty vectors, indicating success
        assertThat(failures(post(index, payload, Map.of("refresh", "wait_for", "error_trace", "true"))), empty());
        assertCount(index, (long) batchCount * rowCount);
    }

    /**
     * Document ids come from the {@code _id} column, and failed items are reported in the Arrow response with the
     * number of the row they come from.
     */
    public void testIdsAndFailures() throws Exception {
        String index = "arrow_bulk_failures";
        createBatchIndex(index);

        assertThat(failures(post(index, idsStream("a"))), empty());
        // The second row has the id of an existing document, and operations default to "create"
        assertThat(failures(post(index, idsStream("x", "a", "y"))), equalTo(List.of(new Failure(1, 409))));
        assertCountAfterConflicts(index, 3);
    }

    /** Failures are numbered from the start of the stream, across record batches. */
    public void testFailureRowNumbersSpanRecordBatches() throws Exception {
        String index = "arrow_bulk_failures_batches";
        createBatchIndex(index);
        assertThat(failures(post(index, idsStream("a", "b"))), empty());

        // Two record batches of two rows each. Row 1 (batch 0) and row 2 (batch 1) have the id of an existing document.
        String[][] ids = { { "c", "a" }, { "b", "d" } };
        byte[] payload = arrowStream(new Schema(List.of(stringField(ArrowBulkAction.ID), intField("ints"))), 2, (root, batch) -> {
            for (int i = 0; i < 2; i++) {
                ((VarCharVector) root.getVector(0)).setSafe(i, new Text(ids[batch][i]));
                ((IntVector) root.getVector(1)).setSafe(i, batch * 2 + i);
            }
            root.setRowCount(2);
        });

        HoldBulksPlugin.bulkRequests.set(0);
        assertThat(failures(post(index, payload)), equalTo(List.of(new Failure(1, 409), new Failure(2, 409))));
        assertThat("both record batches are sent in a single bulk request", HoldBulksPlugin.bulkRequests.get(), equalTo(1));
        assertCountAfterConflicts(index, 4);
    }

    /** With {@code op_type=index}, a row with an existing id replaces the document instead of failing. */
    public void testOpTypeIndexOverwrites() throws Exception {
        String index = "arrow_bulk_op_type";
        createBatchIndex(index);

        // The value of "ints" is the number of the row: the document "a" has 1
        assertThat(failures(post(index, idsStream("z", "a"))), empty());
        assertThat(failures(post(index, idsStream("a"), Map.of("refresh", "true", "op_type", "index"))), empty());
        assertCount(index, 2);
        // It now has 0
        assertEquals(0, ((Number) client().prepareGet(index, "a").get().getSourceAsMap().get("ints")).intValue());
    }

    /**
     * The {@code _index} column sets the target of every row, which are split into one bulk request per index. Rows without
     * an index use the one in the request path.
     */
    public void testIndexColumn() throws Exception {
        createBatchIndex("arrow_idx_a");
        createBatchIndex("arrow_idx_b");
        createBatchIndex("arrow_idx_default");

        String[] indices = { "arrow_idx_a", "arrow_idx_b", "arrow_idx_a", null, "arrow_idx_b", "arrow_idx_a" };
        byte[] payload = arrowStream(new Schema(List.of(stringField(ArrowBulkAction.INDEX), intField("ints"))), 1, (root, batch) -> {
            for (int i = 0; i < indices.length; i++) {
                if (indices[i] == null) {
                    ((VarCharVector) root.getVector(0)).setNull(i);
                } else {
                    ((VarCharVector) root.getVector(0)).setSafe(i, new Text(indices[i]));
                }
                ((IntVector) root.getVector(1)).setSafe(i, i);
            }
            root.setRowCount(indices.length);
        });

        assertThat(failures(post("arrow_idx_default", payload)), empty());
        assertCount("arrow_idx_a", 3);
        assertCount("arrow_idx_b", 2);
        assertCount("arrow_idx_default", 1);
    }

    /** Failures of rows that were split between indices are reported with the number of the row that was sent. */
    public void testFailureRowNumbersWithIndexColumn() throws Exception {
        createBatchIndex("arrow_idx_fail_a");
        createBatchIndex("arrow_idx_fail_b");
        assertThat(failures(post("arrow_idx_fail_a", idsStream("x"))), empty());

        String[] indices = { "arrow_idx_fail_b", "arrow_idx_fail_a", "arrow_idx_fail_a", "arrow_idx_fail_b" };
        String[] ids = { "x", "y", "x", "y" };
        byte[] payload = arrowStream(
            new Schema(List.of(stringField(ArrowBulkAction.INDEX), stringField(ArrowBulkAction.ID), intField("ints"))),
            1,
            (root, batch) -> {
                for (int i = 0; i < indices.length; i++) {
                    ((VarCharVector) root.getVector(0)).setSafe(i, new Text(indices[i]));
                    ((VarCharVector) root.getVector(1)).setSafe(i, new Text(ids[i]));
                    ((IntVector) root.getVector(2)).setSafe(i, i);
                }
                root.setRowCount(indices.length);
            }
        );

        // Only row 2 conflicts: the document "x" already exists in index "a". Id "x" in index "b" is a different document.
        assertThat(failures(post(null, payload)), equalTo(List.of(new Failure(2, 409))));
        assertCountAfterConflicts("arrow_idx_fail_a", 2);
        assertCount("arrow_idx_fail_b", 2);
    }

    /**
     * Values of the supported types are indexed and fetched back as expected, including structs, lists, dates and nulls.
     */
    public void testDocumentContent() throws Exception {
        String index = "arrow_content";
        createBatchIndex(
            index,
            "name",
            "type=keyword",
            "count",
            "type=long",
            "score",
            "type=double",
            "active",
            "type=boolean",
            "ts",
            "type=date",
            "obj.a",
            "type=integer",
            "obj.b",
            "type=keyword",
            "tags",
            "type=keyword"
        );

        var schema = new Schema(
            List.of(
                stringField(ArrowBulkAction.ID),
                stringField("name"),
                field("count", new ArrowType.Int(64, true)),
                field("score", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
                field("active", ArrowType.Bool.INSTANCE),
                field("ts", new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)),
                field("obj", ArrowType.Struct.INSTANCE, intField("a"), stringField("b")),
                field("tags", ArrowType.List.INSTANCE, stringField("$data$"))
            )
        );

        byte[] payload = arrowStream(schema, 1, (root, batch) -> {
            ((VarCharVector) root.getVector(ArrowBulkAction.ID)).setSafe(0, new Text("full"));
            ((VarCharVector) root.getVector("name")).setSafe(0, new Text("alice"));
            ((BigIntVector) root.getVector("count")).setSafe(0, 5);
            ((Float8Vector) root.getVector("score")).setSafe(0, 1.5);
            ((BitVector) root.getVector("active")).setSafe(0, 1);
            ((TimeStampMilliVector) root.getVector("ts")).setSafe(0, 1_700_000_000_000L);
            var obj = (StructVector) root.getVector("obj");
            ((IntVector) obj.getChild("a")).setSafe(0, 1);
            ((VarCharVector) obj.getChild("b")).setSafe(0, new Text("x"));
            obj.setIndexDefined(0);
            var tags = (ListVector) root.getVector("tags");
            tags.startNewValue(0);
            ((VarCharVector) tags.getDataVector()).setSafe(0, new Text("a"));
            ((VarCharVector) tags.getDataVector()).setSafe(1, new Text("b"));
            tags.endValue(0, 2);

            // The second row only has a count: everything else is null
            ((VarCharVector) root.getVector(ArrowBulkAction.ID)).setSafe(1, new Text("sparse"));
            ((BigIntVector) root.getVector("count")).setSafe(1, 6);
            for (String name : List.of("name", "score", "active", "ts", "obj", "tags")) {
                root.getVector(name).setNull(1);
            }
            ((IntVector) obj.getChild("a")).setSafe(1, 99);
            for (var vector : root.getFieldVectors()) {
                vector.setValueCount(2);
            }
            root.setRowCount(2);
        });

        assertThat(failures(post(index, payload)), empty());

        var full = client().prepareGet(index, "full").get().getSourceAsMap();
        assertThat(full.get("name"), equalTo("alice"));
        assertThat(((Number) full.get("count")).longValue(), equalTo(5L));
        assertThat(full.get("score"), equalTo(1.5));
        assertThat(full.get("active"), equalTo(true));
        assertThat(full.get("ts"), equalTo("2023-11-14T22:13:20.000Z"));
        if (usesColumnarPath()) {
            // The columnar index mode doesn't use subobjects: the fields of objects are returned with their full name
            assertThat(full.get("obj.a"), equalTo(1));
            assertThat(full.get("obj.b"), equalTo("x"));
        } else {
            assertThat(full.get("obj"), equalTo(Map.of("a", 1, "b", "x")));
        }
        assertThat(full.get("tags"), equalTo(List.of("a", "b")));

        // Nulls are absent values, and the child of a null struct is ignored
        var sparse = client().prepareGet(index, "sparse").get().getSourceAsMap();
        assertThat(sparse.keySet(), equalTo(java.util.Set.of("count")));

        // Fields are searchable, not just stored
        assertHitCount(client().prepareSearch(index).setQuery(org.elasticsearch.index.query.QueryBuilders.termQuery("obj.b", "x")), 1);
        assertHitCount(client().prepareSearch(index).setQuery(org.elasticsearch.index.query.QueryBuilders.termQuery("tags", "b")), 1);
    }

    /** Checks that the index of the test class is indexed the way the test class expects, so that both paths are really tested. */
    public void testIndexingPath() throws Exception {
        String index = "arrow_path";
        createBatchIndex(index);

        Logger batchLogger = LogManager.getLogger(ShardBatchIndexer.class);
        Level originalLevel = batchLogger.getLevel();
        Loggers.setLevel(batchLogger, Level.TRACE);
        try (var mockLog = MockLog.capture(ShardBatchIndexer.class)) {
            String message = "batch indexed * operations on primary shard *";
            mockLog.addExpectation(
                usesColumnarPath()
                    ? new MockLog.SeenEventExpectation("columnar path", ShardBatchIndexer.class.getName(), Level.TRACE, message)
                    : new MockLog.UnseenEventExpectation("no columnar path", ShardBatchIndexer.class.getName(), Level.TRACE, message)
            );
            assertThat(failures(post(index, idsStream("a", "b", "c"))), empty());
            mockLog.assertAllExpectationsMatched();
        } finally {
            Loggers.setLevel(batchLogger, originalLevel);
        }
        assertCount(index, 3);
    }

    /** An empty stream, and a stream whose record batches have no rows, index nothing and succeed. */
    public void testNoRows() throws Exception {
        String index = "arrow_no_rows";
        createBatchIndex(index);

        byte[] payload = arrowStream(new Schema(List.of(intField("ints"))), 2, (root, batch) -> root.setRowCount(0));
        assertThat(failures(post(index, payload)), empty());
        assertCount(index, 0);
    }

    /** Documents of an Arrow stream are routed to the shards of a data stream. */
    public void testDataStream() throws Exception {
        String dataStream = "logs-arrow-ds";
        putDataStreamTemplate(dataStream, """
            { "properties": { "@timestamp": { "type": "date" }, "host": { "type": "keyword" }, "value": { "type": "long" } } }
            """, indexModeSettings());
        createDataStream(dataStream);

        int rows = randomIntBetween(10, 50);
        Instant now = Instant.now();
        byte[] payload = arrowStream(
            new Schema(
                List.of(
                    field("@timestamp", new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)),
                    stringField("host"),
                    field("value", new ArrowType.Int(64, true))
                )
            ),
            2,
            (root, batch) -> {
                for (int i = 0; i < rows; i++) {
                    ((TimeStampMilliVector) root.getVector(0)).setSafe(i, now.toEpochMilli() + i);
                    ((VarCharVector) root.getVector(1)).setSafe(i, new Text("host" + (i % 3)));
                    ((BigIntVector) root.getVector(2)).setSafe(i, batch * rows + i);
                }
                root.setRowCount(rows);
            }
        );

        assertThat(failures(post(dataStream, payload, Map.of("refresh", "true", "require_data_stream", "true"))), empty());
        assertHitCount(client().prepareSearch(dataStream).setSize(0).setTrackTotalHits(true), 2L * rows);
    }

    public void testBatchIndexingDisabledIsRejected() throws Exception {
        String index = "arrow_disabled";
        createBatchIndex(index);
        updateClusterSettings(Settings.builder().put(BatchIndexingEnabled.BATCH_INDEXING.getKey(), false).build());
        try {
            var e = expectFailure(() -> post(index, idsStream("a")));
            assertBadRequest(e, "requires batch indexing");
        } finally {
            updateClusterSettings(Settings.builder().putNull(BatchIndexingEnabled.BATCH_INDEXING.getKey()).build());
        }
        assertCount(index, 0);
    }

    private long refreshCount(String index) {
        return indicesAdmin().prepareStats(index).setRefresh(true).get().getPrimaries().getRefresh().getTotal();
    }

    /**
     * The refresh of a request is done once for the whole stream, and not once per bulk request that is created for each record batch
     * (with {@code wait_for}, every bulk request would wait for a refresh). The documents of all shards are visible when the
     * response is received, whatever the shards that the last record batch was written to.
     */
    public void testRefreshIsDoneOnce() throws Exception {
        int shards = 3;
        int recordBatches = 6;
        int rows = 60;
        byte[] payload = rowsStream(recordBatches, rows);

        for (String refresh : new String[] { "true", "wait_for", "false" }) {
            String index = "arrow_refresh_" + refresh;
            // Without automatic refresh, only the refresh of the request can make documents visible
            createBatchIndex(
                index,
                Settings.builder().put("index.number_of_shards", shards).put("index.refresh_interval", "-1"),
                "ints",
                "type=integer",
                "strings",
                "type=keyword"
            );
            long before = refreshCount(index);

            assertThat(failures(post(index, payload, Map.of("refresh", refresh))), empty());

            long refreshes = refreshCount(index) - before;
            if (refresh.equals("false")) {
                assertThat("no refresh was requested", refreshes, equalTo(0L));
            } else {
                // One refresh of the shards. A refresh for each of the record batches would be at least six times more.
                assertThat("refreshes with refresh=" + refresh, refreshes, lessThanOrEqualTo(2L * shards));
                assertCount(index, (long) recordBatches * rows);
            }
        }
    }

    public void testRejectedRequests() throws Exception {
        String index = "arrow_rejected";
        createBatchIndex(index);

        // Ingest pipelines can't process batches
        assertBadRequest(expectFailure(() -> post(index, idsStream("a"), Map.of("pipeline", "p"))), "pipelines are not supported");

        // No target index, neither in the path nor in the data
        assertBadRequest(expectFailure(() -> post(null, idsStream("a"))), "No target index");

        // Invalid refresh policy, rejected before any data is read
        assertBadRequest(expectFailure(() -> post(index, idsStream("a"), Map.of("refresh", "nope"))), "Unknown value for refresh");

        // Invalid sizes of bulk requests
        assertBadRequest(
            expectFailure(() -> post(index, idsStream("a"), Map.of("bulk_rows", "0"))),
            "[bulk_rows] must be greater than zero"
        );
        assertBadRequest(expectFailure(() -> post(index, idsStream("a"), Map.of("bulk_rows", "abc"))), "bulk_rows");
        assertBadRequest(expectFailure(() -> post(index, idsStream("a"), Map.of("bulk_bytes", "0"))), "[bulk_bytes] must be between");
        assertBadRequest(expectFailure(() -> post(index, idsStream("a"), Map.of("bulk_bytes", "1gb"))), "[bulk_bytes] must be between");
        assertBadRequest(expectFailure(() -> post(index, idsStream("a"), Map.of("bulk_bytes", "xyz"))), "bulk_bytes");

        // Invalid operation type
        assertBadRequest(expectFailure(() -> post(index, idsStream("a"), Map.of("op_type", "delete"))), "[op_type] must be");

        // Other bulk actions aren't supported
        byte[] action = arrowStream(new Schema(List.of(stringField(ArrowBulkAction.ID), stringField("_bulk_action"))), 1, (root, batch) -> {
            ((VarCharVector) root.getVector(0)).setSafe(0, new Text("a"));
            ((VarCharVector) root.getVector(1)).setSafe(0, new Text("delete"));
            root.setRowCount(1);
        });
        assertBadRequest(expectFailure(() -> post(index, action)), "_bulk_action");

        // Arrow types that can't be converted are rejected, instead of being indexed as something else
        byte[] decimal = arrowStream(new Schema(List.of(field("dec", new ArrowType.Decimal(10, 2, 128)))), 1, (root, batch) -> {
            ((org.apache.arrow.vector.DecimalVector) root.getVector(0)).setSafe(0, 100);
            root.setRowCount(1);
        });
        assertBadRequest(expectFailure(() -> post(index, decimal)), "is not supported for ingestion");

        // Not an Arrow stream
        var request = new Request("POST", "/_arrow/" + index + "/_bulk");
        request.setOptions(request.getOptions().toBuilder().addHeader("Content-type", Arrow.MEDIA_TYPE));
        request.setEntity(
            new ByteArrayEntity("this is not Arrow".getBytes(java.nio.charset.StandardCharsets.UTF_8), ContentType.create(Arrow.MEDIA_TYPE))
        );
        assertThat(
            expectThrows(ResponseException.class, () -> restClient.performRequest(request)).getResponse().getStatusLine().getStatusCode(),
            equalTo(400)
        );

        // Nothing was indexed by rejected requests
        assertCount(index, 0);
    }

    // ---------------------------------------------------------------------------------------------------------------------------------
    // Failures in the middle of a stream

    /**
     * Asserts that no memory is retained after a request: neither the Arrow buffers of the parsed stream, nor the pages of the
     * ESCF batches that were created from them, including those of bulk requests that were never executed.
     */
    protected static void assertNoLeaks(long arrowBaseline) throws Exception {
        // Resources are released when the request is completed, which can be slightly after the response was sent
        assertBusy(() -> assertThat("Arrow memory leaked", Arrow.rootAllocator().getAllocatedMemory(), equalTo(arrowBaseline)));
        MockPageCacheRecycler.ensureAllPagesAreReleased();
        MockBigArrays.ensureAllArraysAreReleased();
    }

    /**
     * A record batch that is invalid after other record batches were read: the first bulk request may already have been
     * executed (depending on how the data was chunked), and everything that remains must be released. The rows of the first record
     * batches wait to be sent, until there are enough of them.
     */
    public void testFailureInSecondRecordBatchDoesNotLeak() throws Exception {
        // The rows of the first record batch wait in memory when the second one fails
        assertFailureInSecondRecordBatchDoesNotLeak(ArrowBulkParser.DEFAULT_BULK_ROWS);
        // The first record batch was sent: it's a bulk request that was created but wasn't executed yet when the second one fails
        assertFailureInSecondRecordBatchDoesNotLeak(1);
    }

    private void assertFailureInSecondRecordBatchDoesNotLeak(int bulkRows) throws Exception {
        String index = "arrow_leak_batches_" + bulkRows;
        createBatchIndex(index);
        long baseline = Arrow.rootAllocator().getAllocatedMemory();
        int rows = 10;

        // The second record batch has a row without a target index, and the request path doesn't provide a default
        byte[] payload = arrowStream(new Schema(List.of(stringField(ArrowBulkAction.INDEX), intField("ints"))), 3, (root, batch) -> {
            for (int row = 0; row < rows; row++) {
                if (batch == 1 && row == rows / 2) {
                    ((VarCharVector) root.getVector(0)).setNull(row);
                } else {
                    ((VarCharVector) root.getVector(0)).setSafe(row, new Text(index));
                }
                ((IntVector) root.getVector(1)).setSafe(row, batch * rows + row);
            }
            root.setRowCount(rows);
        });

        for (boolean chunked : new boolean[] { false, true }) {
            var params = Map.of("refresh", "true", "bulk_rows", String.valueOf(bulkRows));
            assertBadRequest(expectFailure(() -> post(null, payload, params, chunked)), "No target index");
            assertNoLeaks(baseline);
        }

        // The third record batch was never read. The first one was indexed, or not, depending on how the request was received.
        long indexed = count(index);
        assertThat(indexed % rows, equalTo(0L));
        assertThat(indexed, lessThanOrEqualTo(2L * rows));
    }

    /**
     * A record batch whose rows go to several indices becomes several bulk requests. When the conversion of the second one
     * fails, the first one hasn't been executed yet, and its batch must be released with the request that wasn't sent.
     */
    public void testFailureAfterFirstBulkWasBuiltDoesNotLeak() throws Exception {
        createBatchIndex("arrow_leak_a");
        createBatchIndex("arrow_leak_b");
        long baseline = Arrow.rootAllocator().getAllocatedMemory();

        // Ids can be strings or integers. The rows of "arrow_leak_a" have no id, which is fine, but a floating point id isn't valid
        // and is found when the rows of "arrow_leak_b" are converted, after those of "arrow_leak_a".
        var schema = new Schema(
            List.of(
                stringField(ArrowBulkAction.INDEX),
                field(ArrowBulkAction.ID, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
                intField("ints")
            )
        );
        int rows = 20;
        byte[] payload = arrowStream(schema, 1, (root, batch) -> {
            for (int row = 0; row < rows; row++) {
                boolean a = row < rows / 2;
                ((VarCharVector) root.getVector(0)).setSafe(row, new Text(a ? "arrow_leak_a" : "arrow_leak_b"));
                if (a) {
                    ((Float8Vector) root.getVector(1)).setNull(row);
                } else {
                    ((Float8Vector) root.getVector(1)).setSafe(row, 1.5);
                }
                ((IntVector) root.getVector(2)).setSafe(row, row);
            }
            root.setRowCount(rows);
        });

        for (boolean chunked : new boolean[] { false, true }) {
            assertBadRequest(
                expectFailure(() -> post(null, payload, Map.of("refresh", "true"), chunked)),
                "must be a string or an integer"
            );
            assertNoLeaks(baseline);
        }

        // A request that fails while being parsed doesn't execute any of its bulk requests
        assertCount("arrow_leak_a", 0);
        assertCount("arrow_leak_b", 0);
    }

    /** A stream that ends in the middle of a message. */
    public void testTruncatedStreamDoesNotLeak() throws Exception {
        createBatchIndex("arrow_leak_truncated");
        long baseline = Arrow.rootAllocator().getAllocatedMemory();

        int rows = 10;
        byte[] payload = arrowStream(new Schema(List.of(intField("ints"), stringField("strings"))), 4, (root, batch) -> {
            for (int row = 0; row < rows; row++) {
                ((IntVector) root.getVector(0)).setSafe(row, batch * rows + row);
                ((VarCharVector) root.getVector(1)).setSafe(row, new Text("row" + row));
            }
            root.setRowCount(rows);
        });
        // Remove the end of stream marker and part of the last record batch
        byte[] truncated = java.util.Arrays.copyOf(payload, payload.length - 40);

        for (boolean chunked : new boolean[] { false, true }) {
            var e = expectFailure(() -> post("arrow_leak_truncated", truncated, Map.of("refresh", "true"), chunked));
            assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
            assertNoLeaks(baseline);
        }

        long indexed = count("arrow_leak_truncated");
        assertThat(indexed % rows, equalTo(0L));
        assertThat(indexed, lessThanOrEqualTo(3L * rows));
    }

    // ---------------------------------------------------------------------------------------------------------------------------------
    // Coalescing of record batches

    /**
     * The rows of small record batches are sent together, in bulk requests of about {@code bulk_rows} rows, or when the stream ends.
     */
    public void testSmallRecordBatchesAreCoalesced() throws Exception {
        String index = "arrow_coalesce";
        createBatchIndex(index);

        // 25 record batches of 40 rows: rows are sent after the 10th and the 20th record batch (400 rows), and at the end of the stream
        HoldBulksPlugin.bulkRequests.set(0);
        assertThat(failures(post(index, rowsStream(25, 40), Map.of("refresh", "true", "bulk_rows", "400"))), empty());
        assertThat(HoldBulksPlugin.bulkRequests.get(), equalTo(3));
        assertCount(index, 1_000);

        // With the default, fewer rows than the limit are sent when the stream ends
        String small = "arrow_coalesce_small";
        createBatchIndex(small);
        HoldBulksPlugin.bulkRequests.set(0);
        assertThat(failures(post(small, rowsStream(50, 100))), empty());
        assertThat(HoldBulksPlugin.bulkRequests.get(), equalTo(1));
        assertCount(small, 5_000);
    }

    /** A record batch is never split, even if it has more rows than {@code bulk_rows}. */
    public void testRecordBatchBiggerThanBulkRowsIsNotSplit() throws Exception {
        String index = "arrow_coalesce_big";
        createBatchIndex(index);

        HoldBulksPlugin.bulkRequests.set(0);
        assertThat(failures(post(index, rowsStream(1, 3_500), Map.of("refresh", "true", "bulk_rows", "1000"))), empty());
        assertThat(HoldBulksPlugin.bulkRequests.get(), equalTo(1));
        assertCount(index, 3_500);
    }

    /** What waits is sent when it is bigger than {@code bulk_bytes}, however few rows there are. */
    public void testBulkBytesParameter() throws Exception {
        String index = "arrow_coalesce_bytes";
        createBatchIndex(index);

        // With the smallest value, every record batch is sent as soon as it is read
        HoldBulksPlugin.bulkRequests.set(0);
        assertThat(failures(post(index, rowsStream(10, 100), Map.of("refresh", "true", "bulk_bytes", "1b"))), empty());
        assertThat(HoldBulksPlugin.bulkRequests.get(), equalTo(10));
        assertCount(index, 1_000);

        // Sizes with units are accepted, and a limit that isn't reached doesn't send anything before the end of the stream
        HoldBulksPlugin.bulkRequests.set(0);
        assertThat(failures(post(index, rowsStream(10, 100), Map.of("refresh", "true", "bulk_bytes", "60mb"))), empty());
        assertThat(HoldBulksPlugin.bulkRequests.get(), equalTo(1));
        assertCount(index, 2_000);
    }

    /** The rows of each target are accumulated across record batches: one bulk request per index for the stream. */
    public void testIndexColumnAcrossRecordBatches() throws Exception {
        createBatchIndex("arrow_coalesce_a");
        createBatchIndex("arrow_coalesce_b");

        int rows = 10;
        byte[] payload = arrowStream(new Schema(List.of(stringField(ArrowBulkAction.INDEX), intField("ints"))), 3, (root, batch) -> {
            for (int row = 0; row < rows; row++) {
                // The index alternates for every row, and the first one of each record batch is different
                String index = (row + batch) % 2 == 0 ? "arrow_coalesce_a" : "arrow_coalesce_b";
                ((VarCharVector) root.getVector(0)).setSafe(row, new Text(index));
                ((IntVector) root.getVector(1)).setSafe(row, batch * rows + row);
            }
            root.setRowCount(rows);
        });

        HoldBulksPlugin.bulkRequests.set(0);
        assertThat(failures(post(null, payload)), empty());
        assertThat(HoldBulksPlugin.bulkRequests.get(), equalTo(2));
        assertCount("arrow_coalesce_a", 15);
        assertCount("arrow_coalesce_b", 15);
    }

    /** Rows with the same id are indexed in the order of the stream, also when they are in different record batches. */
    public void testCoalescingKeepsRowOrder() throws Exception {
        String index = "arrow_coalesce_order";
        createBatchIndex(index);

        // The same document in three record batches, with the number of the record batch as value
        byte[] payload = arrowStream(new Schema(List.of(stringField(ArrowBulkAction.ID), intField("ints"))), 3, (root, batch) -> {
            ((VarCharVector) root.getVector(0)).setSafe(0, new Text("x"));
            ((IntVector) root.getVector(1)).setSafe(0, batch);
            root.setRowCount(1);
        });

        HoldBulksPlugin.bulkRequests.set(0);
        assertThat(failures(post(index, payload, Map.of("refresh", "true", "op_type", "index"))), empty());
        assertThat(HoldBulksPlugin.bulkRequests.get(), equalTo(1));
        assertCount(index, 1);
        assertEquals(2, ((Number) client().prepareGet(index, "x").get().getSourceAsMap().get("ints")).intValue());
    }

    // ---------------------------------------------------------------------------------------------------------------------------------
    // Bulk requests that run at the same time

    /** Without document ids, the bulk requests of a stream run at the same time, up to a limit, while the data is parsed. */
    public void testBulkRequestsOverlapWithoutIds() throws Exception {
        String index = "arrow_overlap";
        createBatchIndex(index);

        int recordBatches = 2 * ArrowBulkAction.MAX_IN_FLIGHT_BULKS;
        int rows = 20;
        byte[] payload = rowsStream(recordBatches, rows);
        boolean chunked = randomBoolean();

        // Every record batch is sent as a bulk request when it is read, and they are held to see how many run at the same time
        HoldBulksPlugin.holding = true;
        var response = CompletableFuture.supplyAsync(() -> {
            try {
                return post(index, payload, Map.of("refresh", "true", "bulk_rows", "1"), chunked);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
        try {
            assertBusy(() -> assertThat(HoldBulksPlugin.held.size(), equalTo(ArrowBulkAction.MAX_IN_FLIGHT_BULKS)));
        } finally {
            // Let all of them complete. The ones that weren't sent yet aren't held anymore
            HoldBulksPlugin.holding = false;
            HoldBulksPlugin.releaseAll();
        }

        assertThat(failures(response.get(30, java.util.concurrent.TimeUnit.SECONDS)), empty());
        assertCount(index, (long) recordBatches * rows);
    }

    /**
     * With document ids, bulk requests run one at a time, in the order of the stream: documents with the same id can overwrite
     * each other, and the last one must win.
     */
    public void testBulkRequestsWithIdsRunOneAtATime() throws Exception {
        String index = "arrow_sequential";
        createBatchIndex(index);

        // The same document in every record batch, with the number of the record batch as value
        int recordBatches = 2 * ArrowBulkAction.MAX_IN_FLIGHT_BULKS;
        byte[] payload = arrowStream(
            new Schema(List.of(stringField(ArrowBulkAction.ID), intField("ints"))),
            recordBatches,
            (root, batch) -> {
                ((VarCharVector) root.getVector(0)).setSafe(0, new Text("x"));
                ((IntVector) root.getVector(1)).setSafe(0, batch);
                root.setRowCount(1);
            }
        );
        boolean chunked = randomBoolean();

        HoldBulksPlugin.holding = true;
        var response = CompletableFuture.supplyAsync(() -> {
            try {
                return post(index, payload, Map.of("refresh", "true", "op_type", "index", "bulk_rows", "1"), chunked);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
        try {
            for (int i = 0; i < recordBatches; i++) {
                // The next bulk request is only sent when the previous one has completed
                assertBusy(() -> assertThat(HoldBulksPlugin.held.size(), greaterThanOrEqualTo(1)));
                assertThat("one bulk request at a time", HoldBulksPlugin.held.size(), equalTo(1));
                HoldBulksPlugin.releaseAll();
            }
        } finally {
            HoldBulksPlugin.holding = false;
            HoldBulksPlugin.releaseAll();
        }

        assertThat(failures(response.get(30, java.util.concurrent.TimeUnit.SECONDS)), empty());
        assertCount(index, 1);
        assertEquals(recordBatches - 1, ((Number) client().prepareGet(index, "x").get().getSourceAsMap().get("ints")).intValue());
    }

    // ---------------------------------------------------------------------------------------------------------------------------------
    // Client disconnections

    private long openHttpConnections() {
        return getInstanceFromNode(HttpServerTransport.class).stats().getServerOpen();
    }

    /**
     * Starts a streamed request with {@code firstChunk} as the beginning of the body, and without the end of the stream. Calls
     * {@code whileConnected}, then closes the connection abruptly, as a client that crashed or gave up would, calls
     * {@code afterDisconnect}, and waits until the node noticed the disconnection.
     * <p>
     * A node that applies back-pressure doesn't read from the connection while it is busy with the data it has already received (here,
     * while a bulk request is running), so it notices the disconnection only after that, when it is ready to read again.
     * This is why {@code afterDisconnect} is the place to let a bulk request that is running complete.
     */
    private void streamAndDisconnect(
        String index,
        byte[] firstChunk,
        CheckedRunnable<Exception> whileConnected,
        CheckedRunnable<Exception> afterDisconnect
    ) throws Exception {
        long openBefore = openHttpConnections();
        var address = httpAddress();
        try (var socket = new Socket(address.getAddress(), address.getPort())) {
            socket.setSoLinger(true, 0); // closing sends a reset
            OutputStream out = socket.getOutputStream();
            String headers = "POST /_arrow/" + index
            // Every record batch is sent as soon as it is read
                + "/_bulk?refresh=true&bulk_rows=1 HTTP/1.1\r\nHost: localhost\r\nContent-Type: "
                + Arrow.MEDIA_TYPE
                + "\r\nTransfer-Encoding: chunked\r\n\r\n"
                + Integer.toHexString(firstChunk.length)
                + "\r\n";
            out.write(headers.getBytes(java.nio.charset.StandardCharsets.US_ASCII));
            out.write(firstChunk);
            out.write("\r\n".getBytes(java.nio.charset.StandardCharsets.US_ASCII));
            out.flush();

            whileConnected.run();
        }
        afterDisconnect.run();
        assertBusy(() -> assertThat("the node didn't notice that the client disconnected", openHttpConnections(), equalTo(openBefore)));
    }

    private byte[] rowsStream(int recordBatches, int rows) throws IOException {
        return arrowStream(new Schema(List.of(intField("ints"), stringField("strings"))), recordBatches, (root, batch) -> {
            for (int row = 0; row < rows; row++) {
                ((IntVector) root.getVector(0)).setSafe(row, batch * rows + row);
                ((VarCharVector) root.getVector(1)).setSafe(row, new Text("row" + row));
            }
            root.setRowCount(rows);
        });
    }

    /**
     * The client disconnects while a bulk request is running, and the bulk request completes afterwards. The request is closed when
     * the node notices the disconnection, around the time the bulk request completes. Nothing must be used after it was released,
     * and nothing must leak, including the batches of the bulk requests that weren't sent.
     */
    public void testDisconnectWhileBulkIsRunning() throws Exception {
        for (int recordBatches : new int[] { 1, 3 }) {
            String index = "arrow_disconnect_" + recordBatches;
            createBatchIndex(index);
            long arrowBaseline = Arrow.rootAllocator().getAllocatedMemory();

            int rows = 20;
            byte[] stream = rowsStream(recordBatches, rows);
            // Without the end of stream marker (8 bytes), the request is still waiting for data. With several record batches, those
            // after the first one are parsed and waiting for the first bulk request to complete.
            byte[] firstChunk = Arrays.copyOf(stream, stream.length - 8);

            HoldBulksPlugin.holding = true;
            streamAndDisconnect(
                index,
                firstChunk,
                () -> assertBusy(
                    // Without ids, the bulk requests run at the same time: there is one for each record batch
                    () -> assertThat(
                        "a bulk request per record batch should be running",
                        HoldBulksPlugin.held.size(),
                        equalTo(recordBatches)
                    )
                ),
                // The client is gone, and the bulk request completes now
                () -> {
                    HoldBulksPlugin.holding = false;
                    HoldBulksPlugin.releaseAll();
                }
            );

            // At least the bulk request that was running is indexed. The following ones are executed or dropped, depending on whether
            // the node noticed the disconnection before it was their turn.
            assertBusy(() -> assertThat(count(index), greaterThanOrEqualTo((long) rows)));
            assertNoLeaks(arrowBaseline);
            long indexed = count(index);
            assertThat(indexed % rows, equalTo(0L));
            assertThat(indexed, lessThanOrEqualTo((long) recordBatches * rows));

            // The node still works
            assertThat(failures(post(index, idsStream("after_disconnect"))), empty());
        }
    }

    /**
     * The client disconnects in the middle of an Arrow message, after some record batches were received: nothing that was built from
     * them must leak, and the node keeps working. The HTTP chunks that were received but not parsed are Netty buffers, whose release
     * can't be checked here.
     */
    public void testDisconnectInTheMiddleOfAMessage() throws Exception {
        String index = "arrow_disconnect_message";
        createBatchIndex(index);
        long arrowBaseline = Arrow.rootAllocator().getAllocatedMemory();

        int rows = 20;
        int recordBatches = 3;
        byte[] stream = rowsStream(recordBatches, rows);
        // Cut inside the last record batch, that is the biggest part of the stream after the first ones
        byte[] firstChunk = Arrays.copyOf(stream, stream.length - 30);

        streamAndDisconnect(index, firstChunk, () -> {}, () -> {});

        assertNoLeaks(arrowBaseline);
        // The complete record batches before the cut were indexed or not, depending on when the node noticed the disconnection
        long indexed = count(index);
        assertThat(indexed % rows, equalTo(0L));
        assertThat(indexed, lessThanOrEqualTo((long) (recordBatches - 1) * rows));
        assertThat(failures(post(index, idsStream("after_disconnect"))), empty());
    }

    // ---------------------------------------------------------------------------------------------------------------------------------
    // Data streams

    protected void putDataStreamTemplate(String dataStream, String mapping, Settings.Builder settings) throws IOException {
        var request = new TransportPutComposableIndexTemplateAction.Request(dataStream + "-template");
        request.indexTemplate(
            ComposableIndexTemplate.builder()
                .indexPatterns(List.of(dataStream + "*"))
                .template(
                    new Template(
                        settings.put("index.number_of_shards", 2)
                            .put("index.number_of_replicas", 0)
                            .put(IndexSettings.BATCH_INDEXING.getKey(), true)
                            .build(),
                        CompressedXContent.fromJSON(mapping),
                        null
                    )
                )
                .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                .build()
        );
        assertAcked(client().execute(TransportPutComposableIndexTemplateAction.TYPE, request));
    }

    protected void createDataStream(String dataStream) {
        assertAcked(
            client().execute(
                CreateDataStreamAction.INSTANCE,
                new CreateDataStreamAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, dataStream)
            )
        );
        ensureGreen(dataStream);
    }
}
