/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.arrow.bulk;

import org.elasticsearch.arrow.bulk.BulkPipeline.Kind;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class BulkPipelineTests extends ESTestCase {

    /** Runs the steps until there's nothing to do, and returns them as text: "send:2", "chunk", "finish". */
    private static List<String> drain(BulkPipeline<Integer> pipeline) {
        List<String> steps = new ArrayList<>();
        BulkPipeline.Step<Integer> step;
        while ((step = pipeline.next()) != null) {
            steps.add(switch (step.kind()) {
                case SEND -> "send:" + step.bulk();
                case REQUEST_CHUNK -> "chunk";
                case FINISH -> "finish";
            });
        }
        return steps;
    }

    public void testFirstThingToDoIsToRequestData() {
        var pipeline = new BulkPipeline<Integer>(() -> 4);
        assertThat(drain(pipeline), equalTo(List.of("chunk")));
        // It is only requested once
        assertThat(drain(pipeline), equalTo(List.of()));
    }

    public void testBulkRequestsRunAtTheSameTimeUpToTheLimit() {
        var pipeline = new BulkPipeline<Integer>(() -> 2);
        drain(pipeline);

        // A chunk that has 5 bulk requests. Two are sent, and there is no need for more data until the others were sent.
        for (int i = 0; i < 5; i++) {
            pipeline.add(i);
        }
        pipeline.chunkReceived(false);
        assertThat(drain(pipeline), equalTo(List.of("send:0", "send:1")));
        assertThat(pipeline.inFlight(), equalTo(2));

        // Each completion sends the next one
        pipeline.bulkCompleted();
        assertThat(drain(pipeline), equalTo(List.of("send:2")));
        pipeline.bulkCompleted();
        assertThat(drain(pipeline), equalTo(List.of("send:3")));
        pipeline.bulkCompleted();
        // The last waiting one is sent, and more data can be requested as it ran out of waiting bulk requests
        assertThat(drain(pipeline), equalTo(List.of("send:4", "chunk")));
        assertThat(pipeline.inFlight(), equalTo(2));
    }

    public void testNextDataIsRequestedWhileBulkRequestsRun() {
        var pipeline = new BulkPipeline<Integer>(() -> 1);
        drain(pipeline);

        // One bulk request for the first chunk: it is sent, and the next chunk is requested without waiting for it to complete
        pipeline.add(0);
        pipeline.chunkReceived(false);
        assertThat(drain(pipeline), equalTo(List.of("send:0", "chunk")));

        // The second chunk is received while the first bulk request runs: it can't be sent yet, and nothing else is requested
        pipeline.add(1);
        pipeline.chunkReceived(false);
        assertThat(drain(pipeline), equalTo(List.of()));
        assertThat("only one at a time", pipeline.inFlight(), equalTo(1));

        pipeline.bulkCompleted();
        assertThat(drain(pipeline), equalTo(List.of("send:1", "chunk")));
    }

    public void testChunkWithoutBulkRequestsRequestsMoreData() {
        var pipeline = new BulkPipeline<Integer>(() -> 4);
        drain(pipeline);
        pipeline.chunkReceived(false);
        assertThat(drain(pipeline), equalTo(List.of("chunk")));
    }

    public void testFinishesOnceEverythingCompleted() {
        var pipeline = new BulkPipeline<Integer>(() -> 4);
        drain(pipeline);

        pipeline.add(0);
        pipeline.add(1);
        pipeline.chunkReceived(true);
        // No more data is requested, and the request isn't finished while bulk requests run
        assertThat(drain(pipeline), equalTo(List.of("send:0", "send:1")));
        pipeline.bulkCompleted();
        assertThat(drain(pipeline), equalTo(List.of()));
        pipeline.bulkCompleted();
        assertThat(drain(pipeline), equalTo(List.of("finish")));
        // And only once
        assertThat(pipeline.next(), nullValue());
    }

    public void testFinishesWithoutBulkRequests() {
        var pipeline = new BulkPipeline<Integer>(() -> 4);
        drain(pipeline);
        pipeline.chunkReceived(true);
        assertThat(drain(pipeline), equalTo(List.of("finish")));
    }

    /** The whole body is available at once, without data being requested: the first thing that happens is the last chunk. */
    public void testWholeBodyAtOnce() {
        var pipeline = new BulkPipeline<Integer>(() -> 2);
        for (int i = 0; i < 3; i++) {
            pipeline.add(i);
        }
        pipeline.chunkReceived(true);
        assertThat(drain(pipeline), equalTo(List.of("send:0", "send:1")));
        pipeline.bulkCompleted();
        assertThat(drain(pipeline), equalTo(List.of("send:2")));
        pipeline.bulkCompleted();
        pipeline.bulkCompleted();
        assertThat(drain(pipeline), equalTo(List.of("finish")));
    }

    /** The limit is evaluated for every bulk request: it can be learned from the data, after the first record batch. */
    public void testLimitIsEvaluatedWhenSending() {
        var limit = new AtomicInteger(4);
        var pipeline = new BulkPipeline<Integer>(limit::get);
        drain(pipeline);

        limit.set(1);
        for (int i = 0; i < 3; i++) {
            pipeline.add(i);
        }
        pipeline.chunkReceived(false);
        assertThat(drain(pipeline), equalTo(List.of("send:0")));
        pipeline.bulkCompleted();
        assertThat(drain(pipeline), equalTo(List.of("send:1")));
    }

    public void testBulkRequestsThatWaitCanBeRemoved() {
        var pipeline = new BulkPipeline<Integer>(() -> 1);
        drain(pipeline);
        for (int i = 0; i < 3; i++) {
            pipeline.add(i);
        }
        pipeline.chunkReceived(false);
        assertThat(drain(pipeline), equalTo(List.of("send:0")));

        assertThat(pipeline.drainPending(), equalTo(List.of(1, 2)));
        assertThat(pipeline.drainPending(), equalTo(List.of()));
        pipeline.bulkCompleted();
        assertThat(drain(pipeline), equalTo(List.of("chunk")));
    }

    public void testKindsAreAllCovered() {
        // A reminder to update the tests above when a step is added
        assertThat(Kind.values().length, equalTo(3));
    }
}
