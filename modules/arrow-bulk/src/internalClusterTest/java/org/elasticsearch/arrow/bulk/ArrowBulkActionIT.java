/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.arrow.bulk;

import org.elasticsearch.common.settings.Settings;

/**
 * Arrow bulk ingestion into indices in the standard index mode. Their mappers don't support columnar parsing, so batches are
 * materialized and indexed one document at a time on the primary shard.
 */
public class ArrowBulkActionIT extends ArrowBulkTestCase {

    @Override
    protected Settings.Builder indexModeSettings() {
        return Settings.builder().put("index.mapping.source.mode", "synthetic");
    }

    @Override
    protected boolean usesColumnarPath() {
        return false;
    }
}
