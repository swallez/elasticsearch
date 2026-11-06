/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.arrow.bulk;

import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.index.Index;

import java.util.HashMap;
import java.util.Map;

/**
 * How the shards of the target of a stream are laid out, which determines how big bulk requests should be.
 * <p>
 * A bulk request waits for the translog of every shard it writes to to be synced. The syncs of shards that are on the same node
 * go to the same disk and can wait for each other, so their number is the load of the bulk request on that disk. The more shards
 * of the target a node holds, the more rows a bulk request needs for the cost of these syncs to be shared between enough rows.
 */
final class TargetShards {

    private TargetShards() {}

    /**
     * The highest number of copies of shards (primaries and replicas) of {@code target} that are assigned to a single node. The
     * target is the name of an index, an alias or a data stream, in which case the index that documents are written to is used.
     * <p>
     * Returns 1 if there is nothing to count: the target doesn't exist yet (it will be created when documents are written),
     * has no write index, or has no shard assigned.
     */
    static int maxCopiesPerNode(ProjectState project, String target) {
        IndexAbstraction abstraction = project.metadata().getIndicesLookup().get(target);
        if (abstraction == null) {
            return 1;
        }
        Index writeIndex = abstraction.getWriteIndex();
        if (writeIndex == null) {
            return 1;
        }
        var indexRoutingTable = project.routingTable().index(writeIndex);
        if (indexRoutingTable == null) {
            return 1;
        }
        Map<String, Integer> copiesPerNode = new HashMap<>();
        int max = 1;
        for (var iterator = indexRoutingTable.allShards().iterator(); iterator.hasNext();) {
            for (ShardRouting shard : iterator.next().assignedShards()) {
                max = Math.max(max, copiesPerNode.merge(shard.currentNodeId(), 1, Integer::sum));
            }
        }
        return max;
    }
}
