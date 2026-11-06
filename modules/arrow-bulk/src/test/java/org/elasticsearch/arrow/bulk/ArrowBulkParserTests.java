/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.arrow.bulk;

import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.arrow.Arrow;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.BytesRefRecycler;
import org.junit.After;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Tests when the rows of the record batches of an Arrow stream are sent as bulk requests. The conversion of the values is tested in
 * {@link ArrowToEscfTests}, and the whole endpoint in the integration tests.
 * <p>
 * The pages of the batches come from a recycler that checks that they are all released, which fails a test that leaks them.
 */
public class ArrowBulkParserTests extends ESTestCase {

    private static final String DEFAULT_INDEX = "default-index";

    private final List<ArrowBulkParser.PendingBulk> bulks = new ArrayList<>();

    /** The consumer of the bulk requests is responsible for closing them. This runs before the check that no pages leaked. */
    @After
    public void releaseBulks() {
        for (var bulk : bulks) {
            bulk.request().releasePreBuiltBatches();
        }
    }

    private static ArrowBulkParser.Params params(@Nullable String defaultIndex, int bulkRows, long bulkBytes) {
        return new ArrowBulkParser.Params(
            defaultIndex,
            null,
            DocWriteRequest.OpType.CREATE,
            false,
            false,
            TimeValue.timeValueSeconds(10),
            null,
            bulkRows,
            bulkBytes,
            Set.of()
        );
    }

    private ArrowBulkParser parser(int bulkRows, long bulkBytes) {
        return parser(params(DEFAULT_INDEX, bulkRows, bulkBytes), () -> true);
    }

    private ArrowBulkParser parser(ArrowBulkParser.Params params, BooleanSupplier supported) {
        var recycler = new BytesRefRecycler(new MockPageCacheRecycler(Settings.EMPTY));
        return new ArrowBulkParser(params, recycler, supported, bulks::add);
    }

    private static Field field(String name, ArrowType type) {
        return new Field(name, FieldType.nullable(type), null);
    }

    /**
     * An Arrow stream whose record batches have {@code rows[i]} rows. The first column is an integer that is the number of the row in
     * the stream, and the other columns are metadata: the value of {@code metadata} is called with the vector and the number of the row,
     * for the columns that are named by {@code metadataColumns}.
     */
    private static byte[] stream(int[] rows, List<String> metadataColumns, BiConsumer<VarCharVector, Integer> metadata) throws IOException {
        List<Field> fields = new ArrayList<>();
        fields.add(field("n", new ArrowType.Int(32, true)));
        for (String name : metadataColumns) {
            fields.add(field(name, new ArrowType.Utf8()));
        }
        var out = new ByteArrayOutputStream();
        try (
            var allocator = Arrow.newChildAllocator("test", 0, Long.MAX_VALUE);
            var root = VectorSchemaRoot.create(new Schema(fields), allocator)
        ) {
            try (var writer = new ArrowStreamWriter(root, null, out)) {
                int next = 0;
                for (int count : rows) {
                    for (int row = 0; row < count; row++) {
                        ((IntVector) root.getVector(0)).setSafe(row, next);
                        for (int column = 0; column < metadataColumns.size(); column++) {
                            metadata.accept((VarCharVector) root.getVector(column + 1), next);
                        }
                        next++;
                    }
                    root.setRowCount(count);
                    writer.writeBatch();
                }
            }
        }
        return out.toByteArray();
    }

    private static byte[] stream(int... rows) throws IOException {
        return stream(rows, List.of(), (vector, row) -> {});
    }

    /** Parses data that arrives in pieces of the given size, as the chunks of an HTTP request. */
    private static void parseInChunks(ArrowBulkParser parser, byte[] data, int chunkSize) throws IOException {
        int consumed = 0;
        int available = 0;
        while (available < data.length) {
            available = Math.min(data.length, available + chunkSize);
            boolean last = available == data.length;
            consumed += parser.parse(new BytesArray(data, consumed, available - consumed), last);
        }
    }

    private int totalRows() {
        return bulks.stream().mapToInt(bulk -> bulk.request().requests().size()).sum();
    }

    public void testRowsAreSentWhenTheStreamEnds() throws IOException {
        try (var parser = parser(1_000, Long.MAX_VALUE)) {
            parser.parse(new BytesArray(stream(10, 10, 10)), false);
            assertThat("the rows wait for more rows", bulks.size(), equalTo(0));

            parser.parse(new BytesArray(new byte[0]), true);
            assertThat(bulks.size(), equalTo(1));
        }
        var bulk = bulks.get(0);
        assertThat(bulk.index(), equalTo(DEFAULT_INDEX));
        assertThat(bulk.request().requests().size(), equalTo(30));
        assertTrue(bulk.request().hasPreBuiltBatches());
        // The numbers of the rows in the stream, to report failures
        assertThat(bulk.rowNumbers()[0], equalTo(0L));
        assertThat(bulk.rowNumbers()[29], equalTo(29L));
    }

    public void testRowsAreSentWhenThereAreEnoughOfThem() throws IOException {
        try (var parser = parser(25, Long.MAX_VALUE)) {
            // 10 rows per record batch: rows are sent after the third record batch (30 rows), and the remaining ones at the end
            parser.parse(new BytesArray(stream(10, 10, 10, 10, 10)), true);
        }
        assertThat(bulks.size(), equalTo(2));
        assertThat(bulks.get(0).request().requests().size(), equalTo(30));
        assertThat(bulks.get(1).request().requests().size(), equalTo(20));
        // The row numbers are those of the stream, whatever the bulk request
        assertThat(bulks.get(1).rowNumbers()[0], equalTo(30L));
        assertThat(bulks.get(1).rowNumbers()[19], equalTo(49L));
    }

    public void testRecordBatchIsNeverSplit() throws IOException {
        try (var parser = parser(10, Long.MAX_VALUE)) {
            parser.parse(new BytesArray(stream(35)), true);
        }
        assertThat(bulks.size(), equalTo(1));
        assertThat(bulks.get(0).request().requests().size(), equalTo(35));
    }

    public void testRowsAreSentWhenTheyAreTooBig() throws IOException {
        // The limit is on the size of the Arrow buffers: the 40 rows of integers are about 200 bytes, so nothing is sent before the end
        try (var parser = parser(Integer.MAX_VALUE, 5_000)) {
            parser.parse(new BytesArray(stream(10, 10, 10, 10)), true);
        }
        assertThat(bulks.size(), equalTo(1));
        releaseBulks();
        bulks.clear();

        // With a low limit, every record batch is sent
        try (var parser = parser(Integer.MAX_VALUE, 1)) {
            parser.parse(new BytesArray(stream(10, 10, 10, 10)), true);
        }
        assertThat(bulks.size(), equalTo(4));
        assertThat(totalRows(), equalTo(40));
    }

    public void testRowsOfEachIndexAreAccumulated() throws IOException {
        // The target alternates for every row, so that each record batch has rows for the two indices
        byte[] stream = stream(new int[] { 10, 10, 10 }, List.of(ArrowBulkParser.INDEX), (vector, row) -> {
            vector.setSafe(row % 10, new Text(row % 2 == 0 ? "even" : "odd"));
        });
        try (var parser = parser(1_000, Long.MAX_VALUE)) {
            parser.parse(new BytesArray(stream), true);
        }
        assertThat(bulks.size(), equalTo(2));
        assertThat(bulks.get(0).index(), equalTo("even"));
        assertThat(bulks.get(1).index(), equalTo("odd"));
        for (var bulk : bulks) {
            assertThat(bulk.request().requests().size(), equalTo(15));
        }
        // The rows of an index are in the order of the stream
        assertThat(bulks.get(0).rowNumbers()[0], equalTo(0L));
        assertThat(bulks.get(0).rowNumbers()[1], equalTo(2L));
        assertThat(bulks.get(0).rowNumbers()[14], equalTo(28L));
        assertThat(bulks.get(1).rowNumbers()[0], equalTo(1L));
        assertThat(bulks.get(1).rowNumbers()[14], equalTo(29L));
    }

    public void testIdsAreKeptInOrder() throws IOException {
        byte[] stream = stream(new int[] { 2, 2 }, List.of(ArrowBulkParser.ID), (vector, row) -> {
            if (row == 1) {
                vector.setNull(row % 2);
            } else {
                vector.setSafe(row % 2, new Text("id" + row));
            }
        });
        try (var parser = parser(1_000, Long.MAX_VALUE)) {
            parser.parse(new BytesArray(stream), true);
        }
        assertThat(bulks.size(), equalTo(1));
        List<String> ids = new ArrayList<>();
        for (DocWriteRequest<?> request : bulks.get(0).request().requests()) {
            assertThat(request, instanceOf(IndexRequest.class));
            ids.add(request.id());
        }
        // The null id is generated later
        assertThat(ids, equalTo(java.util.Arrays.asList("id0", null, "id2", "id3")));
    }

    public void testChunkedData() throws IOException {
        byte[] stream = stream(7, 7, 7, 7, 7, 7);
        try (var parser = parser(20, Long.MAX_VALUE)) {
            parseInChunks(parser, stream, randomIntBetween(1, 200));
        }
        // 7 rows per record batch: sent after the third (21 rows), and at the end
        assertThat(bulks.stream().map(b -> b.request().requests().size()).toList(), equalTo(List.of(21, 21)));
    }

    /** Rows that wait to be sent when the stream fails are dropped, and their pages released. */
    public void testWaitingRowsAreReleasedOnFailure() throws IOException {
        // The second record batch has a row without index: there is no default index, so it fails after the first one was accumulated
        byte[] stream = stream(new int[] { 10, 10 }, List.of(ArrowBulkParser.INDEX), (vector, row) -> {
            if (row == 15) {
                vector.setNull(row % 10);
            } else {
                vector.setSafe(row % 10, new Text("some-index"));
            }
        });
        try (var parser = parser(params(null, 1_000, Long.MAX_VALUE), () -> true)) {
            var e = expectThrows(IllegalArgumentException.class, () -> parser.parse(new BytesArray(stream), true));
            assertThat(e.getMessage(), org.hamcrest.Matchers.containsString("No target index"));
            assertThat("the rows of the first record batch were not sent", bulks.size(), equalTo(0));
        }
        // ESTestCase checks that all the pages were released
    }

    public void testBatchIndexingMustBeSupported() throws IOException {
        try (var parser = parser(params(DEFAULT_INDEX, 1, 1), () -> false)) {
            var e = expectThrows(IllegalArgumentException.class, () -> parser.parse(new BytesArray(stream(5)), true));
            assertThat(e.getMessage(), org.hamcrest.Matchers.containsString("requires batch indexing"));
        }
        assertThat(bulks.size(), equalTo(0));
    }
}
