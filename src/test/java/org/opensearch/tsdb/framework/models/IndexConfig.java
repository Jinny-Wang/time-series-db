/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.framework.models;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

/**
 * Index configuration for time series testing.
 *
 * <p>The optional {@code cluster} field specifies which cluster the index should be created on.
 * If not specified or set to "local", the index is created on the local (coordinator) cluster.
 * For multi-cluster tests, specify a remote cluster alias (e.g., "cluster_a").
 *
 * <h3>Usage Example:</h3>
 * <pre>{@code
 * index_configs:
 *   - name: "local_metrics"
 *     shards: 1
 *     replicas: 0
 *   - cluster: "cluster_a"
 *     name: "remote_metrics"
 *     shards: 2
 *     replicas: 0
 * }</pre>
 *
 * @param name Index name (required)
 * @param shards Number of primary shards
 * @param replicas Number of replica shards
 * @param settings Optional custom index settings
 * @param mapping Optional custom index mapping
 * @param cluster Optional cluster alias (null or "local" for local cluster)
 */
public record IndexConfig(@JsonProperty("name") String name, @JsonProperty("shards") int shards, @JsonProperty("replicas") int replicas,
    @JsonProperty("settings") Map<String, Object> settings, @JsonProperty("mapping") Map<String, Object> mapping,
    @JsonProperty("cluster") String cluster) {

    /**
     * Returns the cluster alias, defaulting to "local" if not specified.
     */
    public String getCluster() {
        return cluster != null ? cluster : "local";
    }

    /**
     * Returns true if this index should be created on the local cluster.
     */
    public boolean isLocal() {
        return cluster == null || "local".equals(cluster);
    }
}
