/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.translog;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.telemetry.metrics.tags.Tags;
import org.opensearch.tsdb.metrics.TSDBMetrics;

import java.io.IOException;
import java.util.List;

/**
 * Wrapper around DefaultTranslogDeletionPolicy that rate limits the number of translog
 * readers (generations) that can be closed in a single trimming operation.
 * <p>
 * This helps prevent I/O saturation and thread contention when many translog generations need to be trimmed
 * simultaneously by spreading the trimming load across multiple operations.
 * <p>
 * This is helpful to reduce periodic indexing latency spikes which occurs when the translog write lock is held for an
 * extended duration until multiple translog generations are cleaned up. By rate limiting total number of generations
 * cleaned up in one cycle, the write lock is released early and distributes the overhead.
 * <p>
 * Note that the number of readers closed will be based on the current active readers. Delayed cleanup of readers does not
 * add too much overhead, so we decide the number of closed readers dynamically.
 */
public final class RateLimitedTranslogDeletionPolicy extends DefaultTranslogDeletionPolicy {
    private static final Logger logger = LogManager.getLogger(RateLimitedTranslogDeletionPolicy.class);

    private volatile int maxTranslogReadersToClosePercentage;
    private final Tags metricTags;

    /**
     * Creates a new RateLimitedTranslogDeletionPolicy.
     *
     * @param retentionSizeInBytes the retention size in bytes
     * @param retentionAgeInMillis the retention age in milliseconds
     * @param retentionTotalFiles the retention total files
     * @param maxTranslogReadersToClosePercentage the percentage of translog generations allowed to be cleaned up in one trim operation
     * @param metricTags the tags to attach to emitted metrics
     */
    public RateLimitedTranslogDeletionPolicy(
        long retentionSizeInBytes,
        long retentionAgeInMillis,
        int retentionTotalFiles,
        int maxTranslogReadersToClosePercentage,
        Tags metricTags
    ) {
        super(retentionSizeInBytes, retentionAgeInMillis, retentionTotalFiles);
        this.maxTranslogReadersToClosePercentage = maxTranslogReadersToClosePercentage;
        this.metricTags = metricTags;
        logger.info(
            "Initialized RateLimitedTranslogDeletionPolicy with maxTranslogReadersToClosePercentage={}%",
            maxTranslogReadersToClosePercentage
        );
    }

    /**
     * Updates the maximum percentage of translog readers to close per trim operation.
     * This method is called by the settings update consumer registered in the engine.
     *
     * @param newMaxTranslogReadersToClosePercentage the new percentage of translog generations allowed to be cleaned up in one trim operation
     */
    public void setMaxTranslogReadersToClosePercentage(int newMaxTranslogReadersToClosePercentage) {
        this.maxTranslogReadersToClosePercentage = newMaxTranslogReadersToClosePercentage;
        logger.info(
            "Updated RateLimitedTranslogDeletionPolicy with maxTranslogReadersToClosePercentage={}%",
            newMaxTranslogReadersToClosePercentage
        );
    }

    /**
     * Calculates the minimum translog generation that must be retained based on the default deletion policy logic and
     * the configured upper limit by the user.
     *
     * @param readers the list of translog readers
     * @param current the current translog writer
     * @return the adjusted minimum required generation
     * @throws IOException if an I/O error occurs
     */
    @Override
    public synchronized long minTranslogGenRequired(List<TranslogReader> readers, TranslogWriter current) throws IOException {
        // Record total number of translog readers as a metric with index and shard tags
        TSDBMetrics.incrementCounter(TSDBMetrics.ENGINE.translogReadersCount, readers.size(), metricTags);

        // Get the original minimum generation from parent policy
        long originalMinGen = super.minTranslogGenRequired(readers, current);

        // Get the configured percentage limit
        int maxPercentage = maxTranslogReadersToClosePercentage;

        // If percentage is 100 or no readers, no rate limiting needed
        if (maxPercentage >= 100 || readers.isEmpty()) {
            return originalMinGen;
        }

        // Count how many readers would be eligible for closing with the original min generation
        int eligibleReaderCount = 0;
        for (TranslogReader reader : readers) {
            if (reader.getGeneration() < originalMinGen) {
                eligibleReaderCount++;
            }
        }

        // If no readers eligible, no rate limiting needed
        if (eligibleReaderCount == 0) {
            return originalMinGen;
        }

        // Calculate how many readers to allow based on percentage. Use a minimum value of 1.
        int maxReadersToClose = Math.max(1, (eligibleReaderCount * maxPercentage) / 100);

        // If we can close all eligible readers, use original min generation
        if (maxReadersToClose >= eligibleReaderCount) {
            return originalMinGen;
        }

        // Otherwise, adjust min generation to only allow closing maxReadersToClose readers.
        // Readers will be in increasing generation order, so find the reader generation beyond maxReadersToClose
        long adjustedMinGen = readers.get(maxReadersToClose).getGeneration();

        logger.debug(
            "Rate limiting translog trimming: originalMinGen={}, adjustedMinGen={}, "
                + "eligibleReaders={}, maxReadersToClose={}, percentage={}%",
            originalMinGen,
            adjustedMinGen,
            eligibleReaderCount,
            maxReadersToClose,
            maxPercentage
        );

        return adjustedMinGen;
    }
}
