/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.framework.models;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

/**
 * Test setup configuration for time series testing.
 *
 * <p>Supports multiple index configurations for complex test scenarios.
 * Note: Index mapping and settings are provided by the framework, only name/shards/replicas are configurable.
 *
 * <p>For multi-cluster/CCS tests, specify {@code remote_clusters} to define remote clusters
 * that will be created and connected to the local cluster.
 *
 * <h3>Single-Cluster Example:</h3>
 * <pre>{@code
 * test_setup:
 *   name: "Single cluster test"
 *   cluster_config:
 *     nodes: 1
 *   index_configs:
 *     - name: "metrics"
 *       shards: 1
 * }</pre>
 *
 * <h3>Multi-Cluster Example:</h3>
 * <pre>{@code
 * test_setup:
 *   name: "CCS test"
 *   cluster_config:
 *     nodes: 1
 *   remote_clusters:
 *     - alias: "cluster_a"
 *       nodes: 2
 *   index_configs:
 *     - cluster: "cluster_a"
 *       name: "remote_metrics"
 *       shards: 2
 * }</pre>
 *
 * @param name Test setup name
 * @param description Human-readable description
 * @param clusterConfig Configuration for the local cluster
 * @param remoteClusters Optional list of remote cluster configurations for CCS tests
 * @param indexConfigs List of index configurations
 * @param nodeSettings Optional node-level settings
 */
public record TestSetup(@JsonProperty("name") String name, @JsonProperty("description") String description,
    @JsonProperty("cluster_config") ClusterConfig clusterConfig, @JsonProperty("remote_clusters") List<RemoteClusterConfig> remoteClusters,
    @JsonProperty("index_configs") List<IndexConfig> indexConfigs, @JsonProperty("node_settings") Map<String, Object> nodeSettings) {

    /**
     * Returns true if this is a multi-cluster test setup.
     */
    public boolean isMultiCluster() {
        return remoteClusters != null && !remoteClusters.isEmpty();
    }

    /**
     * Returns the list of remote cluster aliases.
     */
    public List<String> getRemoteClusterAliases() {
        if (remoteClusters == null || remoteClusters.isEmpty()) {
            return List.of();
        }
        return remoteClusters.stream().map(RemoteClusterConfig::alias).toList();
    }
}
