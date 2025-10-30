/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage.summarize;

/**
 * Interface for summarizers that aggregate values within a time bucket.
 *
 * <p>A bucket summarizer accumulates values from multiple samples within a time interval
 * and produces a single aggregated result. This is used by the summarize stage to
 * downsample time series data.</p>
 *
 * <h2>Null Handling</h2>
 * <p>In this system, null/missing values are represented by their <strong>absence</strong>
 * from the sample list, not by NaN. When a sample does not exist at a particular timestamp,
 * it simply isn't added to the summarizer.</p>
 *
 * <p>NaN values, if present in the data, are actual values and should be handled according
 * to the aggregation function's semantics (typically excluded from computation).</p>
 *
 * <h2>Usage Pattern</h2>
 * <pre>
 * BucketSummarizer summarizer = new SumBucketSummarizer();
 *
 * // Accumulate values in the bucket
 * for (Sample sample : samplesInBucket) {
 *     summarizer.accumulate(sample.getValue());
 * }
 *
 * // Get result if bucket has data
 * if (summarizer.hasData()) {
 *     double result = summarizer.finish();
 * }
 *
 * // Reset for next bucket
 * summarizer.reset();
 * </pre>
 *
 * <h2>Implementation Types</h2>
 * <ul>
 *   <li><strong>Simple Metric Summarizers:</strong> For sum, avg, min, max, last - only maintain
 *       running statistics without storing all values</li>
 *   <li><strong>Value-Based Summarizers:</strong> For percentiles, stddev - store all values
 *       for final computation</li>
 * </ul>
 */
public interface BucketSummarizer {
    /**
     * Accumulate a value into the current bucket.
     *
     * @param value the value to accumulate (may be NaN, which implementations typically skip)
     */
    void accumulate(double value);

    /**
     * Compute and return the aggregated result for the current bucket.
     * This method should be called after all values have been accumulated.
     *
     * @return the aggregated value, or NaN if no valid data
     */
    double finish();

    /**
     * Reset the summarizer for the next bucket.
     * Clears all accumulated state.
     */
    void reset();

    /**
     * Check if the bucket contains any valid data.
     *
     * @return true if at least one non-NaN value has been accumulated
     */
    boolean hasData();
}
