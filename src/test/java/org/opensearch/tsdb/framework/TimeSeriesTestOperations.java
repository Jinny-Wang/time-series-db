/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.framework;

import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.action.admin.indices.stats.IndexShardStats;
import org.opensearch.action.admin.indices.stats.IndexStats;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.action.admin.indices.stats.ShardStats;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.transport.client.Client;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.framework.models.IndexConfig;
import org.opensearch.tsdb.framework.models.TimeSeriesSample;
import org.opensearch.tsdb.utils.TSDBTestUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Mixin interface providing shared time series test operations.
 *
 * <p>This interface uses the mixin pattern with default methods to share common
 * test operations between single-cluster and multi-cluster test frameworks.
 * Both {@code TimeSeriesTestFramework} and {@code MultiClusterTimeSeriesTestFramework}
 * implement this interface.
 *
 * <h2>Usage</h2>
 * <p>Implementing classes must provide:
 * <ul>
 *   <li>{@link #resolveClient(String)} - returns the appropriate client for a cluster alias</li>
 *   <li>{@link #getDefaultIndexSettings()} - returns default index settings</li>
 *   <li>{@link #getDefaultIndexMapping()} - returns default index mapping</li>
 * </ul>
 *
 * <p>All other methods are provided as default implementations that use these abstractions.
 *
 * <h2>Cluster Alias Convention</h2>
 * <ul>
 *   <li>{@code null} or {@code "local"} - refers to the local/coordinator cluster</li>
 *   <li>Any other value - refers to a remote cluster by its alias</li>
 * </ul>
 */
public interface TimeSeriesTestOperations {

    /** Constant representing the local cluster */
    String LOCAL_CLUSTER = "local";

    // ═══════════════════════════════════════════════════════════════════════
    // ABSTRACT METHODS - Each framework must implement
    // ═══════════════════════════════════════════════════════════════════════

    /**
     * Resolves the client for a given cluster alias.
     *
     * @param clusterAlias the cluster alias (null or "local" for local cluster)
     * @return the Client for the specified cluster
     */
    Client resolveClient(String clusterAlias);

    /**
     * Returns the default index settings to use when creating indices.
     *
     * @return map of setting name to value
     */
    Map<String, Object> getDefaultIndexSettings();

    /**
     * Returns the default index mapping to use when creating indices.
     *
     * @return the mapping configuration
     */
    Map<String, Object> getDefaultIndexMapping();

    // ═══════════════════════════════════════════════════════════════════════
    // INDEX OPERATIONS - Default implementations
    // ═══════════════════════════════════════════════════════════════════════

    /**
     * Creates a time series index on the specified cluster.
     *
     * @param clusterAlias the cluster alias (null or "local" for local cluster)
     * @param indexConfig the index configuration
     */
    default void createTimeSeriesIndex(String clusterAlias, IndexConfig indexConfig) {
        Client client = resolveClient(clusterAlias);

        // Merge default settings with index configuration
        Map<String, Object> allSettings = new HashMap<>(getDefaultIndexSettings());
        if (indexConfig.settings() != null) {
            allSettings.putAll(indexConfig.settings());
        }
        allSettings.put("index.number_of_shards", indexConfig.shards());
        allSettings.put("index.number_of_replicas", indexConfig.replicas());

        Settings settings = Settings.builder().loadFromMap(allSettings).build();

        // Use index-specific mapping if provided, otherwise use default
        Map<String, Object> mappingConfig = indexConfig.mapping() != null ? indexConfig.mapping() : getDefaultIndexMapping();

        client.admin().indices().prepareCreate(indexConfig.name()).setSettings(settings).setMapping(mappingConfig).get();
    }

    /**
     * Creates a time series index on the local cluster.
     *
     * @param indexConfig the index configuration
     */
    default void createTimeSeriesIndex(IndexConfig indexConfig) {
        createTimeSeriesIndex(null, indexConfig);
    }

    /**
     * Deletes an index if it exists on the specified cluster.
     *
     * @param clusterAlias the cluster alias (null or "local" for local cluster)
     * @param indexName the name of the index to delete
     */
    default void deleteIndexIfExists(String clusterAlias, String indexName) {
        Client client = resolveClient(clusterAlias);
        if (client.admin().indices().prepareExists(indexName).get().isExists()) {
            client.admin().indices().prepareDelete(indexName).get();
        }
    }

    /**
     * Deletes an index if it exists on the local cluster.
     *
     * @param indexName the name of the index to delete
     */
    default void deleteIndexIfExists(String indexName) {
        deleteIndexIfExists(null, indexName);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // DATA INGESTION - Default implementations
    // ═══════════════════════════════════════════════════════════════════════

    /**
     * Ingests time series samples into an index on the specified cluster.
     *
     * <p>Uses TSDBDocument format to align with production data flow:
     * <pre>
     * {
     *   "labels": "name http_requests method GET status 200",
     *   "timestamp": 1234567890,
     *   "value": 100.5
     * }
     * </pre>
     *
     * @param clusterAlias the cluster alias (null or "local" for local cluster)
     * @param samples the list of samples to ingest
     * @param indexName the name of the index to ingest data into
     */
    default void ingestSamples(String clusterAlias, List<TimeSeriesSample> samples, String indexName) {
        Client client = resolveClient(clusterAlias);
        BulkRequest bulkRequest = new BulkRequest();

        for (TimeSeriesSample sample : samples) {
            Map<String, String> labels = sample.labels();
            ByteLabels byteLabels = ByteLabels.fromMap(labels);

            try {
                String documentJson = TSDBTestUtils.createTSDBDocumentJson(sample);
                String seriesId = byteLabels.toString();

                IndexRequest request = new IndexRequest(indexName).source(documentJson, XContentType.JSON).routing(seriesId);
                bulkRequest.add(request);
            } catch (java.io.IOException e) {
                throw new RuntimeException("Failed to create TSDB document JSON", e);
            }
        }

        if (bulkRequest.numberOfActions() > 0) {
            BulkResponse bulkResponse = client.bulk(bulkRequest).actionGet();
            if (bulkResponse.hasFailures()) {
                throw new RuntimeException("Bulk ingestion failed: " + bulkResponse.buildFailureMessage());
            }
        }
    }

    /**
     * Ingests time series samples into an index on the local cluster.
     *
     * @param samples the list of samples to ingest
     * @param indexName the name of the index to ingest data into
     */
    default void ingestSamples(List<TimeSeriesSample> samples, String indexName) {
        ingestSamples(null, samples, indexName);
    }

    /**
     * Flushes and refreshes an index on the specified cluster.
     *
     * @param clusterAlias the cluster alias (null or "local" for local cluster)
     * @param indexName the name of the index
     */
    default void flushAndRefresh(String clusterAlias, String indexName) {
        Client client = resolveClient(clusterAlias);
        client.admin().indices().prepareFlush(indexName).get();
        client.admin().indices().prepareRefresh(indexName).get();
    }

    /**
     * Flushes and refreshes an index on the local cluster.
     *
     * @param indexName the name of the index
     */
    default void flushAndRefresh(String indexName) {
        flushAndRefresh(null, indexName);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // VALIDATION - Default implementations
    // ═══════════════════════════════════════════════════════════════════════

    /**
     * Validates shard distribution for an index on the specified cluster.
     *
     * @param clusterAlias the cluster alias (null or "local" for local cluster)
     * @param indexConfig the index configuration
     */
    default void validateShardDistribution(String clusterAlias, IndexConfig indexConfig) {
        Client client = resolveClient(clusterAlias);
        String indexName = indexConfig.name();
        int expectedShards = indexConfig.shards();
        int expectedReplicas = indexConfig.replicas();

        ClusterStateResponse stateResponse = client.admin().cluster().prepareState().setIndices(indexName).get();

        IndexRoutingTable indexRoutingTable = stateResponse.getState().routingTable().index(indexName);

        assertNotNull("Index routing table should exist", indexRoutingTable);
        assertEquals("Number of shards mismatch", expectedShards, indexRoutingTable.shards().size());

        for (int shardId = 0; shardId < expectedShards; shardId++) {
            IndexShardRoutingTable shardRoutingTable = indexRoutingTable.shard(shardId);
            assertNotNull("Shard routing table should exist for shard " + shardId, shardRoutingTable);

            int expectedCopies = 1 + expectedReplicas;
            assertEquals("Shard " + shardId + " should have " + expectedCopies + " copies", expectedCopies, shardRoutingTable.size());

            for (ShardRouting shardRouting : shardRoutingTable) {
                assertTrue("Shard " + shardId + " should be active: " + shardRouting, shardRouting.active());
            }
        }
    }

    /**
     * Validates shard distribution for an index on the local cluster.
     *
     * @param indexConfig the index configuration
     */
    default void validateShardDistribution(IndexConfig indexConfig) {
        validateShardDistribution(null, indexConfig);
    }

    /**
     * Returns document count per shard for an index on the specified cluster.
     *
     * @param clusterAlias the cluster alias (null or "local" for local cluster)
     * @param indexName the name of the index
     * @return map of shard ID to document count
     */
    default Map<Integer, Long> getShardDocumentCounts(String clusterAlias, String indexName) {
        Client client = resolveClient(clusterAlias);
        Map<Integer, Long> shardCounts = new HashMap<>();

        IndicesStatsResponse statsResponse = client.admin().indices().prepareStats(indexName).clear().setDocs(true).get();
        IndexStats indexStats = statsResponse.getIndex(indexName);

        if (indexStats == null) {
            return shardCounts;
        }

        for (IndexShardStats shardStats : indexStats.getIndexShards().values()) {
            int shardId = shardStats.getShardId().id();
            for (ShardStats shard : shardStats.getShards()) {
                if (shard.getShardRouting().primary()) {
                    long docCount = shard.getStats().getDocs().getCount();
                    shardCounts.put(shardId, docCount);
                    break;
                }
            }
        }

        return shardCounts;
    }

    /**
     * Returns document count per shard for an index on the local cluster.
     *
     * @param indexName the name of the index
     * @return map of shard ID to document count
     */
    default Map<Integer, Long> getShardDocumentCounts(String indexName) {
        return getShardDocumentCounts(null, indexName);
    }

    /**
     * Validates minimum document count per shard on the specified cluster.
     *
     * @param clusterAlias the cluster alias (null or "local" for local cluster)
     * @param indexConfig the index configuration
     * @param minDocsPerShard minimum documents per shard
     */
    default void validateDataDistribution(String clusterAlias, IndexConfig indexConfig, int minDocsPerShard) {
        Map<Integer, Long> shardCounts = getShardDocumentCounts(clusterAlias, indexConfig.name());
        int expectedShards = indexConfig.shards();

        assertEquals("Should have document counts for all shards", expectedShards, shardCounts.size());

        for (int shardId = 0; shardId < expectedShards; shardId++) {
            long count = shardCounts.getOrDefault(shardId, 0L);
            assertTrue("Shard " + shardId + " has " + count + " documents, expected at least " + minDocsPerShard, count >= minDocsPerShard);
        }
    }

    /**
     * Validates minimum document count per shard on the local cluster.
     *
     * @param indexConfig the index configuration
     * @param minDocsPerShard minimum documents per shard
     */
    default void validateDataDistribution(IndexConfig indexConfig, int minDocsPerShard) {
        validateDataDistribution(null, indexConfig, minDocsPerShard);
    }

    /**
     * Validates that all shards contain at least one document on the specified cluster.
     *
     * @param clusterAlias the cluster alias (null or "local" for local cluster)
     * @param indexConfig the index configuration
     */
    default void validateAllShardsHaveData(String clusterAlias, IndexConfig indexConfig) {
        validateDataDistribution(clusterAlias, indexConfig, 1);
    }

    /**
     * Validates that all shards contain at least one document on the local cluster.
     *
     * @param indexConfig the index configuration
     */
    default void validateAllShardsHaveData(IndexConfig indexConfig) {
        validateAllShardsHaveData(null, indexConfig);
    }
}
