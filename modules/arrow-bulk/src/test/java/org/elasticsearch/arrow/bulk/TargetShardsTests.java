/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.arrow.bulk;

import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class TargetShardsTests extends ESTestCase {

    private static ProjectState project(ClusterState state) {
        return state.projectState(Metadata.DEFAULT_PROJECT_ID);
    }

    /** All the shards are on the only node: they share its disk. */
    public void testShardsOnTheSameNode() {
        var state = ClusterStateCreationUtils.state("index", 1, 4);
        assertThat(TargetShards.maxCopiesPerNode(project(state), "index"), equalTo(4));
    }

    /** A single shard is never more than one copy on a node, however many nodes there are. */
    public void testSingleShard() {
        var state = ClusterStateCreationUtils.state("index", randomIntBetween(1, 8), 1);
        assertThat(TargetShards.maxCopiesPerNode(project(state), "index"), equalTo(1));
    }

    /** Shards that are spread over nodes: the busiest node counts. */
    public void testShardsSpreadOverNodes() {
        int shards = 6;
        int nodes = 2;
        var state = ClusterStateCreationUtils.state("index", nodes, shards);
        int copies = TargetShards.maxCopiesPerNode(project(state), "index");
        // Whatever the allocation, one of the nodes holds at least half of the shards
        assertThat(copies, greaterThanOrEqualTo(shards / nodes));
        assertThat(copies, lessThanOrEqualTo(shards));
    }

    /** A target that doesn't exist yet will be created with the documents: nothing to count. */
    public void testUnknownTarget() {
        var state = ClusterStateCreationUtils.state("index", 1, 4);
        assertThat(TargetShards.maxCopiesPerNode(project(state), "other"), equalTo(1));
    }

    /** An alias has the shards of its write index. */
    public void testAlias() {
        var state = ClusterStateCreationUtils.state("index", 1, 3);
        ProjectMetadata projectMetadata = state.metadata().getProject(Metadata.DEFAULT_PROJECT_ID);
        IndexMetadata index = projectMetadata.index("index");
        var withAlias = ProjectMetadata.builder(projectMetadata)
            .put(IndexMetadata.builder(index).putAlias(AliasMetadata.builder("alias").writeIndex(true)))
            .build();
        var stateWithAlias = ClusterState.builder(state).putProjectMetadata(withAlias).build();
        assertThat(TargetShards.maxCopiesPerNode(project(stateWithAlias), "alias"), equalTo(3));
    }
}
