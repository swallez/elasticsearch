/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet.parquetrs;

/**
 * JNI bridge to the Rust parquet-rs based Parquet reader.
 * <p>
 * Uses the Arrow C Data Interface for zero-copy batch transfer from Rust to Java.
 * Filter expressions are encoded as FlatBuffers (schema in
 * {@code native/schema/filter_expr.fbs}) and passed to {@link #openReader} as a
 * {@code byte[]}; the native side decodes them on demand. See
 * {@link ParquetRsFilterPushdownSupport} for the Java-side encoder.
 */
final class ParquetRsBridge {

    private ParquetRsBridge() {}

    // ---- Reader lifecycle ----

    /**
     * Opens a parquet-rs reader with optional filter.
     *
     * @param filter FlatBuffers-encoded {@code FilterExpr} payload (see
     *               {@code native/schema/filter_expr.fbs}), or {@code null} / empty for no filter.
     *               The native side decodes the bytes; ownership of the array stays with the JVM.
     * @param configJson JSON-serialized storage configuration from the ESQL WITH clause, or null.
     */
    static native long openReader(
        String filePath,
        String[] projectedColumns,
        int batchSize,
        long limit,
        byte[] filter,
        String configJson
    );

    static native long openReaderMulti(
        String[] filePaths,
        String[] projectedColumns,
        int batchSize,
        long limit,
        byte[] filter,
        String configJson
    );

    static native boolean nextBatch(long handle, long schemaAddr, long arrayAddr);

    static native void closeReader(long handle);

    /** Returns a human-readable description of the reader's scan plan (pushed filter, projection, row groups, etc.). */
    static native String getReaderPlan(long handle);

    // ---- Metadata ----

    /**
     * Exports the Parquet file's Arrow schema via the C Data Interface.
     * The caller must allocate an {@code ArrowSchema} and pass its memory address.
     *
     * @param schemaAddr memory address of a pre-allocated {@code ArrowSchema} FFI struct
     */
    static native void getSchemaFFI(String filePath, String configJson, long schemaAddr);

    static native long[] getStatistics(String filePath, String configJson);

    /** Returns column statistics as [name0, nullCount0, min0, max0, name1, ...]. Empty string = absent. */
    static native String[] getColumnStatistics(String filePath, String configJson);
}