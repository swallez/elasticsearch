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

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Field;

/**
 * Provides primitive array allocation that skips the JLS-mandated zero-fill via
 * {@code jdk.internal.misc.Unsafe#allocateUninitializedArray}. Only meaningful for callers that
 * will overwrite the entire array before reading from it (e.g. {@code BigArrays} with
 * {@code clearOnResize == false}).
 * <p>
 * Requires {@code --add-exports} and {@code --add-opens} for {@code java.base/jdk.internal.misc} on
 * the running JVM; if either is missing the helper transparently falls back to {@code new byte[n]}
 * so callers never need to branch.
 */
final class UninitializedArrays {

    private static final Logger logger = LogManager.getLogger(UninitializedArrays.class);

    /** Bound MethodHandle of signature {@code (int)byte[]} pointing at {@code Unsafe.allocateUninitializedArray(byte.class, n)}, or null. */
    private static final MethodHandle ALLOCATE_UNINITIALIZED_BYTE_ARRAY = resolve();

    private UninitializedArrays() {}

    /**
     * Returns {@code true} when {@link #newByteArray(int)} will skip the JLS zero-fill via
     * the JDK-internal fast path, and {@code false} when it falls back to {@code new byte[n]} (e.g. the
     * required {@code --add-exports}/{@code --add-opens} flags are missing).
     */
    static boolean isEnabled() {
        return ALLOCATE_UNINITIALIZED_BYTE_ARRAY != null;
    }

    private static MethodHandle resolve() {
        try {
            Class<?> unsafeClass = Class.forName("jdk.internal.misc.Unsafe");
            Field theUnsafe = unsafeClass.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            Object unsafe = theUnsafe.get(null);
            MethodHandle raw = MethodHandles.lookup()
                .findVirtual(unsafeClass, "allocateUninitializedArray", MethodType.methodType(Object.class, Class.class, int.class));
            return MethodHandles.insertArguments(raw.bindTo(unsafe), 0, byte.class).asType(MethodType.methodType(byte[].class, int.class));
        } catch (Throwable t) {
            logger.debug("uninitialized array allocation unavailable, falling back to new byte[n]", t);
            return null;
        }
    }

    /**
     * Allocates a {@code byte[]} of the requested size. Contents are undefined when the JDK-internal
     * fast path is available; otherwise the array is zero-initialized as usual. Callers must therefore
     * treat the returned array as containing arbitrary bytes and overwrite before reading.
     */
    static byte[] newByteArray(int size) {
        MethodHandle mh = ALLOCATE_UNINITIALIZED_BYTE_ARRAY;
        if (mh != null) {
            try {
                return (byte[]) mh.invokeExact(size);
            } catch (Throwable ignored) {
                // fall through to safe allocation
            }
        }
        return new byte[size];
    }
}
