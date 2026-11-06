/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.arrow.bulk;

import org.elasticsearch.core.Nullable;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.function.IntSupplier;

/**
 * Decides what to do next with the bulk requests that are created from the data of a stream: send one, ask for more data, or
 * complete the request. It doesn't do anything itself, which makes these decisions easy to test.
 * <p>
 * Several bulk requests can run at the same time, up to a limit, so that parsing and converting the data, and indexing, overlap. The
 * next data is requested as soon as no bulk request waits to be sent, whether bulk requests are running or not. This applies
 * back-pressure to the client, and limits the memory to the bulk requests that run, and those that were created from the last data
 * that was received.
 * <p>
 * This class isn't thread-safe: callers must make sure that it is used by one thread at a time.
 *
 * @param <B> the type of the bulk requests
 */
final class BulkPipeline<B> {

    enum Kind {
        /** Send {@link Step#bulk()}. */
        SEND,
        /** Ask for the next chunk of data. */
        REQUEST_CHUNK,
        /** All the data was received and all the bulk requests have completed. */
        FINISH
    }

    /**
     * What to do.
     *
     * @param bulk the bulk request to send, for {@link Kind#SEND}, and {@code null} otherwise
     */
    record Step<B>(Kind kind, @Nullable B bulk) {}

    private final IntSupplier maxInFlight;
    private final ArrayDeque<B> pending = new ArrayDeque<>();
    private int inFlight;
    private boolean chunkRequested;
    private boolean lastChunk;
    private boolean finished;

    /**
     * @param maxInFlight the number of bulk requests that can run at the same time. It's called every time a bulk request could be
     *                    sent, so that it can depend on what is learned from the data, such as whether it has document ids
     */
    BulkPipeline(IntSupplier maxInFlight) {
        this.maxInFlight = maxInFlight;
    }

    /** A bulk request was created, it will be sent when there is room for it. */
    void add(B bulk) {
        pending.add(bulk);
    }

    /** A chunk of data was received, and everything that can be made of it was added. */
    void chunkReceived(boolean last) {
        chunkRequested = false;
        lastChunk = last;
    }

    /** A bulk request that was sent has completed. */
    void bulkCompleted() {
        assert inFlight > 0 : "no bulk request is running";
        inFlight--;
    }

    /**
     * The next thing to do, or {@code null} if there's nothing to do until something happens. Call it until it returns {@code null}
     * after every event.
     */
    @Nullable
    Step<B> next() {
        if (finished) {
            return null;
        }
        if (pending.isEmpty() == false && inFlight < maxInFlight.getAsInt()) {
            inFlight++;
            return new Step<>(Kind.SEND, pending.poll());
        }
        if (lastChunk && pending.isEmpty() && inFlight == 0) {
            finished = true;
            return new Step<>(Kind.FINISH, null);
        }
        if (lastChunk == false && chunkRequested == false && pending.isEmpty()) {
            chunkRequested = true;
            return new Step<>(Kind.REQUEST_CHUNK, null);
        }
        return null;
    }

    /** Removes and returns the bulk requests that weren't sent, when the request is closed. */
    List<B> drainPending() {
        List<B> drained = new ArrayList<>(pending);
        pending.clear();
        return drained;
    }

    /** The number of bulk requests that were sent and haven't completed. */
    int inFlight() {
        return inFlight;
    }
}
