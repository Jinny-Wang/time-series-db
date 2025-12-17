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
import org.opensearch.test.OpenSearchTestCase;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TSDBSearchMetricsTests extends OpenSearchTestCase {
    private MetricsRegistry registry;
    private TSDBSearchMetrics metrics;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        registry = mock(MetricsRegistry.class);
        metrics = new TSDBSearchMetrics();
    }

    @Override
    public void tearDown() throws Exception {
        metrics.cleanup();
        super.tearDown();
    }

    public void testInitialize() {
        // Create mock counters and histograms
        Counter wildcardCacheHits = mock(Counter.class);
        Counter wildcardCacheMisses = mock(Counter.class);
        Counter wildcardCacheEvictions = mock(Counter.class);
        Histogram wildcardCacheSize = mock(Histogram.class);

        // Setup mocks
        when(
            registry.createCounter(
                eq(TSDBMetricsConstants.SEARCH_WILDCARD_CACHE_HITS_TOTAL),
                eq(TSDBMetricsConstants.SEARCH_WILDCARD_CACHE_HITS_TOTAL_DESC),
                eq(TSDBMetricsConstants.UNIT_COUNT)
            )
        ).thenReturn(wildcardCacheHits);

        when(
            registry.createCounter(
                eq(TSDBMetricsConstants.SEARCH_WILDCARD_CACHE_MISSES_TOTAL),
                eq(TSDBMetricsConstants.SEARCH_WILDCARD_CACHE_MISSES_TOTAL_DESC),
                eq(TSDBMetricsConstants.UNIT_COUNT)
            )
        ).thenReturn(wildcardCacheMisses);

        when(
            registry.createCounter(
                eq(TSDBMetricsConstants.SEARCH_WILDCARD_CACHE_EVICTIONS_TOTAL),
                eq(TSDBMetricsConstants.SEARCH_WILDCARD_CACHE_EVICTIONS_TOTAL_DESC),
                eq(TSDBMetricsConstants.UNIT_COUNT)
            )
        ).thenReturn(wildcardCacheEvictions);

        when(
            registry.createHistogram(
                eq(TSDBMetricsConstants.SEARCH_WILDCARD_CACHE_SIZE),
                eq(TSDBMetricsConstants.SEARCH_WILDCARD_CACHE_SIZE_DESC),
                eq(TSDBMetricsConstants.UNIT_COUNT)
            )
        ).thenReturn(wildcardCacheSize);

        metrics.initialize(registry);

        // Verify all metrics were created
        assertSame(wildcardCacheHits, metrics.wildcardCacheHits);
        assertSame(wildcardCacheMisses, metrics.wildcardCacheMisses);
        assertSame(wildcardCacheEvictions, metrics.wildcardCacheEvictions);
        assertSame(wildcardCacheSize, metrics.wildcardCacheSize);

        // Verify registry interactions
        verify(registry).createCounter(
            TSDBMetricsConstants.SEARCH_WILDCARD_CACHE_HITS_TOTAL,
            TSDBMetricsConstants.SEARCH_WILDCARD_CACHE_HITS_TOTAL_DESC,
            TSDBMetricsConstants.UNIT_COUNT
        );
        verify(registry).createCounter(
            TSDBMetricsConstants.SEARCH_WILDCARD_CACHE_MISSES_TOTAL,
            TSDBMetricsConstants.SEARCH_WILDCARD_CACHE_MISSES_TOTAL_DESC,
            TSDBMetricsConstants.UNIT_COUNT
        );
        verify(registry).createCounter(
            TSDBMetricsConstants.SEARCH_WILDCARD_CACHE_EVICTIONS_TOTAL,
            TSDBMetricsConstants.SEARCH_WILDCARD_CACHE_EVICTIONS_TOTAL_DESC,
            TSDBMetricsConstants.UNIT_COUNT
        );
        verify(registry).createHistogram(
            TSDBMetricsConstants.SEARCH_WILDCARD_CACHE_SIZE,
            TSDBMetricsConstants.SEARCH_WILDCARD_CACHE_SIZE_DESC,
            TSDBMetricsConstants.UNIT_COUNT
        );
    }

    public void testCleanup() {
        // Setup mocks for initialization
        when(
            registry.createCounter(
                eq(TSDBMetricsConstants.SEARCH_WILDCARD_CACHE_HITS_TOTAL),
                eq(TSDBMetricsConstants.SEARCH_WILDCARD_CACHE_HITS_TOTAL_DESC),
                eq(TSDBMetricsConstants.UNIT_COUNT)
            )
        ).thenReturn(mock(Counter.class));

        when(
            registry.createCounter(
                eq(TSDBMetricsConstants.SEARCH_WILDCARD_CACHE_MISSES_TOTAL),
                eq(TSDBMetricsConstants.SEARCH_WILDCARD_CACHE_MISSES_TOTAL_DESC),
                eq(TSDBMetricsConstants.UNIT_COUNT)
            )
        ).thenReturn(mock(Counter.class));

        when(
            registry.createCounter(
                eq(TSDBMetricsConstants.SEARCH_WILDCARD_CACHE_EVICTIONS_TOTAL),
                eq(TSDBMetricsConstants.SEARCH_WILDCARD_CACHE_EVICTIONS_TOTAL_DESC),
                eq(TSDBMetricsConstants.UNIT_COUNT)
            )
        ).thenReturn(mock(Counter.class));

        when(
            registry.createHistogram(
                eq(TSDBMetricsConstants.SEARCH_WILDCARD_CACHE_SIZE),
                eq(TSDBMetricsConstants.SEARCH_WILDCARD_CACHE_SIZE_DESC),
                eq(TSDBMetricsConstants.UNIT_COUNT)
            )
        ).thenReturn(mock(Histogram.class));

        metrics.initialize(registry);
        assertNotNull(metrics.wildcardCacheHits);
        assertNotNull(metrics.wildcardCacheMisses);
        assertNotNull(metrics.wildcardCacheEvictions);
        assertNotNull(metrics.wildcardCacheSize);

        metrics.cleanup();

        assertNull(metrics.wildcardCacheHits);
        assertNull(metrics.wildcardCacheMisses);
        assertNull(metrics.wildcardCacheEvictions);
        assertNull(metrics.wildcardCacheSize);
    }

    public void testCleanupBeforeInitialization() {
        // Should not throw
        metrics.cleanup();

        assertNull(metrics.wildcardCacheHits);
        assertNull(metrics.wildcardCacheMisses);
        assertNull(metrics.wildcardCacheEvictions);
        assertNull(metrics.wildcardCacheSize);
    }
}
