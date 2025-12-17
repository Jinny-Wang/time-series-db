/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.metrics;

import org.opensearch.telemetry.metrics.Counter;
import org.opensearch.telemetry.metrics.Histogram;
import org.opensearch.telemetry.metrics.MetricsRegistry;

/**
 * Search-related TSDB metrics (histograms and counters).
 */
public class TSDBSearchMetrics {
    /** Counter for wildcard query cache hits */
    public Counter wildcardCacheHits;

    /** Counter for wildcard query cache misses */
    public Counter wildcardCacheMisses;

    /** Counter for wildcard query cache evictions */
    public Counter wildcardCacheEvictions;

    /** Histogram for wildcard query cache size */
    public Histogram wildcardCacheSize;

    /**
     * Initialize search metrics. Called by TSDBMetrics.initialize().
     */
    public void initialize(MetricsRegistry registry) {
        wildcardCacheHits = registry.createCounter(
            TSDBMetricsConstants.SEARCH_WILDCARD_CACHE_HITS_TOTAL,
            TSDBMetricsConstants.SEARCH_WILDCARD_CACHE_HITS_TOTAL_DESC,
            TSDBMetricsConstants.UNIT_COUNT
        );
        wildcardCacheMisses = registry.createCounter(
            TSDBMetricsConstants.SEARCH_WILDCARD_CACHE_MISSES_TOTAL,
            TSDBMetricsConstants.SEARCH_WILDCARD_CACHE_MISSES_TOTAL_DESC,
            TSDBMetricsConstants.UNIT_COUNT
        );
        wildcardCacheEvictions = registry.createCounter(
            TSDBMetricsConstants.SEARCH_WILDCARD_CACHE_EVICTIONS_TOTAL,
            TSDBMetricsConstants.SEARCH_WILDCARD_CACHE_EVICTIONS_TOTAL_DESC,
            TSDBMetricsConstants.UNIT_COUNT
        );
        wildcardCacheSize = registry.createHistogram(
            TSDBMetricsConstants.SEARCH_WILDCARD_CACHE_SIZE,
            TSDBMetricsConstants.SEARCH_WILDCARD_CACHE_SIZE_DESC,
            TSDBMetricsConstants.UNIT_COUNT
        );
    }

    /**
     * Cleanup search metrics (for tests).
     */
    public void cleanup() {
        wildcardCacheHits = null;
        wildcardCacheMisses = null;
        wildcardCacheEvictions = null;
        wildcardCacheSize = null;
    }
}
