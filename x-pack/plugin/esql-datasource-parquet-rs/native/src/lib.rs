/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

mod filter;
mod filter_fb;
mod generated;
mod jni_utils;
mod metadata;
mod reader;
mod store;

use std::sync::LazyLock;
use tokio::runtime::Runtime;

static ASYNC_RUNTIME: LazyLock<Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .thread_name("parquet-rs-io")
        .enable_all()
        .build()
        .expect("tokio runtime")
});
