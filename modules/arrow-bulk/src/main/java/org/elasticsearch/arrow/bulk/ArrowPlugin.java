/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.arrow.bulk;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.arrow.Arrow;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestHandler;

import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Arrow integration: bulk ingestion of Arrow IPC streams.
 */
public class ArrowPlugin extends Plugin implements ActionPlugin {

    private ClusterService clusterService;
    private Recycler<BytesRef> recycler;

    @Override
    public Collection<?> createComponents(PluginServices services) {
        this.clusterService = services.clusterService();
        this.recycler = services.bigArrays().bytesRefRecycler();

        return List.of(new AbstractLifecycleComponent() {
            @Override
            protected void doStart() {
                // Make sure Arrow is initialized
                Arrow.rootAllocator();
            }

            @Override
            protected void doStop() {}

            @Override
            protected void doClose() {}
        });
    }

    @Override
    public Collection<RestHandler> getRestHandlers(
        RestHandlersServices restHandlersServices,
        Supplier<DiscoveryNodes> nodesInCluster,
        Predicate<NodeFeature> clusterSupportsFeature
    ) {
        return List.of(new ArrowBulkAction(clusterService, recycler));
    }
}
