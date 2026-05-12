/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.util;

import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;

public class PageCacheRecyclerTests extends ESTestCase {

    private static PageCacheRecycler newRecycler() {
        // Default settings allocate a non-trivial heap budget so pages actually get pooled.
        return new PageCacheRecycler(Settings.builder().put(PageCacheRecycler.LIMIT_HEAP_SETTING.getKey(), ByteSizeValue.ofMb(1)).build());
    }

    public void testBytePageWithClearReturnsZeroedFreshPage() {
        PageCacheRecycler recycler = newRecycler();
        Recycler.V<byte[]> v = recycler.bytePage(true);
        try {
            assertEquals(PageCacheRecycler.BYTE_PAGE_SIZE, v.v().length);
            for (byte b : v.v()) {
                assertEquals(0, b);
            }
        } finally {
            v.close();
        }
    }

    public void testBytePageSizeMatchesPageSize() {
        PageCacheRecycler recycler = newRecycler();
        Recycler.V<byte[]> v = recycler.bytePage(randomBoolean());
        try {
            assertEquals(PageCacheRecycler.BYTE_PAGE_SIZE, v.v().length);
        } finally {
            v.close();
        }
    }

    public void testBytePageWithClearZeroesRecycledPage() {
        PageCacheRecycler recycler = newRecycler();

        // First obtain a page, dirty it, return it.
        Recycler.V<byte[]> first = recycler.bytePage(false);
        Arrays.fill(first.v(), (byte) 0x5A);
        first.close();

        // Next obtain with clear=true: contents must be zero even though the page may have been recycled.
        Recycler.V<byte[]> second = recycler.bytePage(true);
        try {
            for (byte b : second.v()) {
                assertEquals(0, b);
            }
        } finally {
            second.close();
        }
    }

    public void testBytePageWithoutClearMayReturnRecycledContents() {
        // We can't assert that contents *are* stale (the recycler may or may not have pooled the page),
        // but we can assert that calling bytePage(false) does not throw and returns a usable buffer.
        PageCacheRecycler recycler = newRecycler();

        Recycler.V<byte[]> first = recycler.bytePage(false);
        byte marker = (byte) 0xAB;
        Arrays.fill(first.v(), marker);
        first.close();

        Recycler.V<byte[]> second = recycler.bytePage(false);
        try {
            assertEquals(PageCacheRecycler.BYTE_PAGE_SIZE, second.v().length);
            // Verify the buffer is writable: overwrite and read back.
            byte newMarker = (byte) ~marker;
            Arrays.fill(second.v(), newMarker);
            for (byte b : second.v()) {
                assertEquals(newMarker, b);
            }
        } finally {
            second.close();
        }
    }

    public void testNonRecyclingInstanceReturnsZeroedPageOnClear() {
        // NON_RECYCLING_INSTANCE always allocates fresh; clear=true must still yield zeros.
        Recycler.V<byte[]> v = PageCacheRecycler.NON_RECYCLING_INSTANCE.bytePage(true);
        try {
            assertEquals(PageCacheRecycler.BYTE_PAGE_SIZE, v.v().length);
            for (byte b : v.v()) {
                assertEquals(0, b);
            }
        } finally {
            v.close();
        }
    }
}
