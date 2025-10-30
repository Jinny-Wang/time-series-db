/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage.summarize;

/**
 * Maps timestamps to bucket start timestamps for summarization.
 *
 * <p>This class encapsulates the bucket alignment logic for a single time series.
 * Buckets are aligned to a reference time, and all timestamps are mapped to their
 * corresponding bucket start based on the interval.</p>
 *
 * <h2>Usage:</h2>
 * <ul>
 *   <li><strong>Fixed alignment:</strong> Use {@link #BucketMapper(long)} to align to a fixed reference (0).
 *       Example: With 1h interval, timestamp 2023-01-01 22:32 maps to bucket 2023-01-01 22:00</li>
 *   <li><strong>Custom alignment:</strong> Use {@link #BucketMapper(long, long)} to align to a specific reference time.
 *       Example: If reference is 2023-01-01 06:30, timestamp 22:32 maps to bucket 22:30</li>
 * </ul>
 */
public class BucketMapper {
    /** Fixed reference time for default alignment (0001-01-01 00:00:00 UTC in milliseconds). */
    private static final long DEFAULT_REFERENCE_TIME = 0L;

    private final long interval;
    private final long referenceTime;

    /**
     * Create a bucket mapper with fixed reference time alignment.
     * Buckets align to interval boundaries from a fixed reference point (0).
     *
     * @param interval bucket interval in the same time unit as timestamps
     */
    public BucketMapper(long interval) {
        this(interval, DEFAULT_REFERENCE_TIME);
    }

    /**
     * Create a bucket mapper with custom reference time alignment.
     * Buckets align to interval boundaries from the specified reference time.
     *
     * @param interval bucket interval in the same time unit as timestamps
     * @param referenceTime reference time for alignment (typically time series start time)
     */
    public BucketMapper(long interval, long referenceTime) {
        if (interval <= 0) {
            throw new IllegalArgumentException("Interval must be positive, got: " + interval);
        }
        this.interval = interval;
        this.referenceTime = referenceTime;
    }

    /**
     * Map a timestamp to its bucket start timestamp.
     *
     * @param timestamp the timestamp to map
     * @return the bucket start timestamp
     */
    public long mapToBucket(long timestamp) {
        // Calculate offset from reference time, then round down to interval
        long offset = timestamp - referenceTime;
        long bucketOffset = (offset / interval) * interval;
        return referenceTime + bucketOffset;
    }

    /**
     * Calculate the bucket start for a given time range.
     *
     * @param minTimestamp minimum timestamp in the range
     * @return bucket start timestamp
     */
    public long calculateBucketStart(long minTimestamp) {
        return mapToBucket(minTimestamp);
    }

    /**
     * Calculate the bucket end (exclusive) for a given time range.
     *
     * @param maxTimestamp maximum timestamp in the range
     * @return bucket end timestamp (exclusive)
     */
    public long calculateBucketEnd(long maxTimestamp) {
        long lastBucketStart = mapToBucket(maxTimestamp);
        return lastBucketStart + interval;
    }

    public long getInterval() {
        return interval;
    }

    public long getReferenceTime() {
        return referenceTime;
    }
}
