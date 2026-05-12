/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.util;

import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.test.ESTestCase;

public class UninitializedArraysTests extends ESTestCase {

    private static final Logger logger = LogManager.getLogger(UninitializedArraysTests.class);

    public void testFastPathIsEnabledUnderTestJvm() {
        // ElasticsearchTestBasePlugin adds --add-exports/--add-opens for java.base/jdk.internal.misc
        // to the test JVM precisely so the fast path is exercised in tests rather than the fallback.
        logger.info("UninitializedArrayAllocator.isEnabled() = {}", UninitializedArrays.isEnabled());
        assertTrue(
            "expected the uninitialized allocation fast path to be enabled under the test JVM; "
                + "if you removed the jdk.internal.misc flags from ElasticsearchTestBasePlugin, update this assertion",
            UninitializedArrays.isEnabled()
        );
    }

    public void testZeroLength() {
        byte[] buf = UninitializedArrays.newByteArray(0);
        assertEquals(0, buf.length);
    }

    public void testRequestedLength() {
        int size = randomIntBetween(1, 1 << 16);
        byte[] buf = UninitializedArrays.newByteArray(size);
        assertEquals(size, buf.length);
    }

    public void testContentsRoundTrip() {
        // The helper makes no guarantee about initial contents; what we *can* verify is that the
        // returned array behaves like an ordinary byte[]: writes are preserved and readable.
        int size = randomIntBetween(1, 4096);
        byte[] buf = UninitializedArrays.newByteArray(size);
        byte[] expected = new byte[size];
        for (int i = 0; i < size; i++) {
            expected[i] = randomByte();
            buf[i] = expected[i];
        }
        assertArrayEquals(expected, buf);
    }

    public void testIndependentAllocations() {
        // Writing into one buffer must not affect another concurrently allocated buffer.
        int size = randomIntBetween(1, 4096);
        byte[] a = UninitializedArrays.newByteArray(size);
        byte[] b = UninitializedArrays.newByteArray(size);
        assertNotSame(a, b);
        byte marker = (byte) (randomByte() | 0x01); // non-zero
        java.util.Arrays.fill(a, marker);
        byte[] aCopy = a.clone();
        java.util.Arrays.fill(b, (byte) ~marker);
        assertArrayEquals(aCopy, a);
    }

    public void testRepeatedAllocations() {
        // Hammer the allocator to flush out any one-shot bugs in the static MethodHandle path.
        for (int iter = 0; iter < 1024; iter++) {
            int size = randomIntBetween(0, 1024);
            byte[] buf = UninitializedArrays.newByteArray(size);
            assertEquals(size, buf.length);
        }
    }
}
