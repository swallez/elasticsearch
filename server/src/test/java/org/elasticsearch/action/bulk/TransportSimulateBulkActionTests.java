/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.ingest.SimulateIndexResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.IndexingPressure;
import org.elasticsearch.indices.EmptySystemIndices;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.test.transport.CapturingTransport;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;

public class TransportSimulateBulkActionTests extends ESTestCase {

    /**
     * Services needed by bulk action
     */
    private TransportService transportService;
    private ClusterService clusterService;
    private TestThreadPool threadPool;

    private TestTransportSimulateBulkAction bulkAction;

    class TestTransportSimulateBulkAction extends TransportSimulateBulkAction {

        TestTransportSimulateBulkAction() {
            super(
                TransportSimulateBulkActionTests.this.threadPool,
                transportService,
                TransportSimulateBulkActionTests.this.clusterService,
                null,
                new ActionFilters(Set.of()),
                new IndexingPressure(Settings.EMPTY),
                EmptySystemIndices.INSTANCE
            );
        }
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getClass().getName());
        DiscoveryNode discoveryNode = DiscoveryNodeUtils.builder("node")
            .version(
                VersionUtils.randomCompatibleVersion(random(), Version.CURRENT),
                IndexVersions.MINIMUM_COMPATIBLE,
                IndexVersionUtils.randomCompatibleVersion(random())
            )
            .build();
        clusterService = createClusterService(threadPool, discoveryNode);
        CapturingTransport capturingTransport = new CapturingTransport();
        transportService = capturingTransport.createTransportService(
            clusterService.getSettings(),
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            boundAddress -> clusterService.localNode(),
            null,
            Collections.emptySet()
        );
        transportService.start();
        transportService.acceptIncomingRequests();
        bulkAction = new TestTransportSimulateBulkAction();
    }

    @After
    public void tearDown() throws Exception {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        threadPool = null;
        clusterService.close();
        super.tearDown();
    }

    public void testIndexData() {
        Task task = mock(Task.class); // unused
        BulkRequest bulkRequest = new SimulateBulkRequest((Map<String, Map<String, Object>>) null);
        int bulkItemCount = randomIntBetween(0, 200);
        for (int i = 0; i < bulkItemCount; i++) {
            Map<String, ?> source = Map.of(randomAlphaOfLength(10), randomAlphaOfLength(5));
            IndexRequest indexRequest = new IndexRequest(randomAlphaOfLength(10)).id(randomAlphaOfLength(10)).source(source);
            indexRequest.setListExecutedPipelines(true);
            for (int j = 0; j < randomIntBetween(0, 10); j++) {
                indexRequest.addPipeline(randomAlphaOfLength(12));
            }
            bulkRequest.add(indexRequest);
        }
        AtomicBoolean onResponseCalled = new AtomicBoolean(false);
        ActionListener<BulkResponse> listener = new ActionListener<>() {
            @Override
            public void onResponse(BulkResponse response) {
                onResponseCalled.set(true);
                BulkItemResponse[] responseItems = response.getItems();
                assertThat(responseItems.length, equalTo(bulkItemCount));
                assertThat(responseItems.length, equalTo(bulkRequest.requests().size()));
                for (int i = 0; i < responseItems.length; i++) {
                    BulkItemResponse responseItem = responseItems[i];
                    IndexRequest indexRequest = (IndexRequest) bulkRequest.requests().get(i);
                    assertNull(responseItem.getFailure());
                    assertThat(responseItem.getResponse(), instanceOf(SimulateIndexResponse.class));
                    SimulateIndexResponse simulateIndexResponse = responseItem.getResponse();
                    assertThat(simulateIndexResponse.getIndex(), equalTo(indexRequest.index()));
                    /*
                     * SimulateIndexResponse doesn't have an equals() method, and most of its state is private. So we check that
                     * its toXContent method produces the expected output.
                     */
                    String output = Strings.toString(simulateIndexResponse);
                    try {
                        assertEquals(
                            XContentHelper.stripWhitespace(
                                Strings.format(
                                    """
                                        {
                                          "_id": "%s",
                                          "_index": "%s",
                                          "_version": -3,
                                          "_source": %s,
                                          "executed_pipelines": [%s]
                                        }""",
                                    indexRequest.id(),
                                    indexRequest.index(),
                                    convertMapToJsonString(indexRequest.sourceAsMap()),
                                    indexRequest.getExecutedPipelines()
                                        .stream()
                                        .map(pipeline -> "\"" + pipeline + "\"")
                                        .collect(Collectors.joining(","))
                                )
                            ),
                            output
                        );
                    } catch (IOException e) {
                        fail(e);
                    }
                }
            }

            @Override
            public void onFailure(Exception e) {
                fail(e, "Unexpected error");
            }
        };
        bulkAction.doInternalExecute(task, bulkRequest, r -> fail("executor is unused"), listener, randomLongBetween(0, Long.MAX_VALUE));
        assertThat(onResponseCalled.get(), equalTo(true));
    }

    private String convertMapToJsonString(Map<String, ?> map) throws IOException {
        try (XContentBuilder builder = JsonXContent.contentBuilder().map(map)) {
            return BytesReference.bytes(builder).utf8ToString();
        }
    }
}
