/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.federation;

/**
 * Provides metadata about federated query execution across multiple clusters or partitions.
 *
 * <p>In federated scenarios, the same time series data may be present across multiple
 * shards, clusters, or partitions. This metadata helps determine whether query operations
 * can be safely pushed down to individual data nodes or must be executed at the
 * coordinator level to ensure correctness.</p>
 *
 * <p>For example, if a time series exists in multiple partitions with overlapping time
 * windows, operations like moving averages must be computed at the coordinator level
 * to have access to all data points across partitions.</p>
 */
public interface FederationMetadata {

    /**
     * Checks if any time series data is present across multiple partitions
     * with overlapping time windows.
     *
     * <p>When this returns true, operations requiring historical context or
     * cross-partition visibility (e.g., moving averages, rate calculations,
     * keepLastValue) should not be pushed down to individual partitions,
     * as each partition would only see partial data and produce incorrect results.</p>
     *
     * @return true if time series data spans multiple partitions with temporal overlap,
     *         false otherwise
     */
    boolean hasOverlappingPartitions();
}
