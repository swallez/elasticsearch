/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.arrow.bulk;

import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.elasticsearch.client.Request;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.sort.SortOrder;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

/**
 * Arrow bulk ingestion into indices in the columnar index mode, whose batches are indexed on the primary shard with the columnar
 * mapper. Time series indices always use it, so their tests are here.
 */
public class ArrowBulkColumnarActionIT extends ArrowBulkTestCase {

    @Override
    protected Settings.Builder indexModeSettings() {
        return Settings.builder().put("index.mode", "columnar");
    }

    @Override
    protected boolean usesColumnarPath() {
        return true;
    }

    /** Time series documents are routed by their dimensions, which are read from the columns of the batch. */
    public void testTimeSeriesDataStream() throws Exception {
        String dataStream = "metrics-arrow-tsdb";
        putDataStreamTemplate(dataStream, """
            { "properties": {
                "@timestamp": { "type": "date" },
                "host": { "type": "keyword", "time_series_dimension": true },
                "cpu": { "type": "double", "time_series_metric": "gauge" }
            } }
            """, Settings.builder().put("index.mode", "time_series"));
        createDataStream(dataStream);

        int hosts = 4;
        int samples = randomIntBetween(2, 10);
        long start = Instant.now().minusSeconds(600).toEpochMilli();
        byte[] payload = arrowStream(
            new Schema(
                List.of(
                    field("@timestamp", new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)),
                    stringField("host"),
                    field("cpu", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE))
                )
            ),
            1,
            (root, batch) -> {
                int row = 0;
                for (int host = 0; host < hosts; host++) {
                    for (int sample = 0; sample < samples; sample++) {
                        ((TimeStampMilliVector) root.getVector(0)).setSafe(row, start + sample * 1000L);
                        ((VarCharVector) root.getVector(1)).setSafe(row, new Text("host" + host));
                        ((Float8Vector) root.getVector(2)).setSafe(row, host + sample / 10.0);
                        row++;
                    }
                }
                root.setRowCount(row);
            }
        );

        assertThat(failures(post(dataStream, payload, Map.of("refresh", "true", "require_data_stream", "true"))), empty());
        assertHitCount(client().prepareSearch(dataStream).setSize(0).setTrackTotalHits(true), (long) hosts * samples);
        // A time series is identified by its dimensions: the same host must have been routed to the same shard every time
        assertHitCount(
            client().prepareSearch(dataStream).setQuery(org.elasticsearch.index.query.QueryBuilders.termQuery("host", "host2")),
            samples
        );

        // Sending the same samples again is rejected as duplicate time series documents, and these are reported as failures
        var again = failures(post(dataStream, payload, Map.of("refresh", "true", "require_data_stream", "true")));
        assertThat(again.size(), equalTo(hosts * samples));
        assertTrue(again.stream().allMatch(f -> f.status() == 409));
    }

    /**
     * A single record batch can contain rows for several backing indices of a time series data stream, as it does when
     * a data stream rolls over: every row must be routed to the backing index whose time range contains its timestamp.
     */
    public void testTimeSeriesRollover() throws Exception {
        String dataStream = "metrics-arrow-rollover";
        putDataStreamTemplate(dataStream, """
            { "properties": {
                "@timestamp": { "type": "date" },
                "host": { "type": "keyword", "time_series_dimension": true },
                "cpu": { "type": "double", "time_series_metric": "gauge" }
            } }
            """, Settings.builder().put("index.mode", "time_series").put("index.look_ahead_time", "30m"));
        createDataStream(dataStream);

        // The first backing index accepts timestamps up to now + 30 minutes. The second one, created by the rollover, starts
        // where the first one ends and spans the following 30 minutes.
        restClient.performRequest(new Request("POST", "/" + dataStream + "/_rollover"));

        long now = Instant.now().toEpochMilli();
        long inFirstIndex = now - 10 * 60_000L;
        long inSecondIndex = now + 45 * 60_000L;

        int rows = 2 * randomIntBetween(5, 20);
        byte[] payload = arrowStream(
            new Schema(
                List.of(
                    field("@timestamp", new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)),
                    stringField("host"),
                    field("cpu", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE))
                )
            ),
            1,
            (root, batch) -> {
                for (int row = 0; row < rows; row++) {
                    // Rows alternate between the two backing indices
                    long base = row % 2 == 0 ? inFirstIndex : inSecondIndex;
                    ((TimeStampMilliVector) root.getVector(0)).setSafe(row, base + row);
                    ((VarCharVector) root.getVector(1)).setSafe(row, new Text("host" + (row % 3)));
                    ((Float8Vector) root.getVector(2)).setSafe(row, row);
                }
                root.setRowCount(rows);
            }
        );

        assertThat(failures(post(dataStream, payload, Map.of("refresh", "true", "require_data_stream", "true"))), empty());

        Map<String, Integer> docsPerIndex = new java.util.HashMap<>();
        assertResponse(client().prepareSearch(dataStream).setSize(rows).addSort("@timestamp", SortOrder.ASC), response -> {
            assertThat(response.getHits().getTotalHits().value(), equalTo((long) rows));
            for (var hit : response.getHits().getHits()) {
                docsPerIndex.merge(hit.getIndex(), 1, Integer::sum);
                var source = hit.getSourceAsMap();
                boolean expectedInFirst = ((Number) source.get("cpu")).intValue() % 2 == 0;
                assertThat(
                    "backing index of the row with timestamp " + source.get("@timestamp"),
                    hit.getIndex().endsWith("-000001"),
                    equalTo(expectedInFirst)
                );
            }
        });
        assertThat(docsPerIndex.size(), equalTo(2));
        assertTrue(docsPerIndex.values().stream().allMatch(count -> count == rows / 2));
    }

}
