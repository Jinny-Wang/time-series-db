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
 * Configuration for a remote cluster in multi-cluster test scenarios.
 *
 * <p>This configuration defines a remote cluster that will be created and connected
 * to the local cluster for cross-cluster search (CCS) testing.
 *
 * <h3>Usage Example:</h3>
 * <pre>{@code
 * remote_clusters:
 *   - alias: "cluster_a"
 *     nodes: 2
 *     settings:
 *       cluster.routing.allocation.disk.threshold_enabled: false
 * }</pre>
 *
 * <p>The alias must match how the cluster is referenced in cross-cluster search queries
 * (e.g., "cluster_a:index_name").
 *
 * @param alias The cluster alias used in CCS queries (e.g., "cluster_a")
 * @param nodes Number of data nodes to create in this cluster (default: 1)
 * @param settings Optional cluster-level settings to apply to all nodes
 */
public record RemoteClusterConfig(@JsonProperty("alias") String alias, @JsonProperty("nodes") Integer nodes, @JsonProperty("settings") Map<
    String,
    Object> settings) {

    /**
     * Returns the number of nodes, defaulting to 1 if not specified.
     */
    public int getNodes() {
        return nodes != null ? nodes : 1;
    }

    /**
     * Validates that the alias is specified and not empty.
     *
     * @throws IllegalArgumentException if alias is null or empty
     */
    public void validate() {
        if (alias == null || alias.trim().isEmpty()) {
            throw new IllegalArgumentException("Remote cluster alias must be specified and non-empty");
        }
    }
}
