/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage.summarize;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Bucket summarizer that computes a percentile of all values.
 *
 * <p>This is a value-based summarizer that stores all values in the bucket
 * and computes the percentile in the finish() method. NaN values are excluded.</p>
 *
 * <p>The percentile calculation follows the same algorithm as PercentileOfSeriesStage:
 * fractionalRank = (percentile / 100) * count, rank = ceil(fractionalRank),
 * result = values[rank - 1] (0-based index).</p>
 */
public class PercentileBucketSummarizer implements BucketSummarizer {
    private final float percentile;
    private final List<Double> values;

    /**
     * Create a percentile summarizer.
     *
     * @param percentile the percentile to calculate (0-100)
     * @throws IllegalArgumentException if percentile is not in [0, 100]
     */
    public PercentileBucketSummarizer(float percentile) {
        if (percentile < 0 || percentile > 100) {
            throw new IllegalArgumentException("Percentile must be between 0 and 100, got: " + percentile);
        }
        this.percentile = percentile;
        this.values = new ArrayList<>();
    }

    @Override
    public void accumulate(double value) {
        if (!Double.isNaN(value)) {
            values.add(value);
        }
    }

    @Override
    public double finish() {
        if (values.isEmpty()) {
            return Double.NaN;
        }

        // Sort values for percentile calculation
        Collections.sort(values);

        // Calculate percentile using same logic as PercentileOfSeriesStage
        double fractionalRank = (percentile / 100.0) * values.size();
        double rank = Math.ceil(fractionalRank);
        int rankAsInt = (int) rank;

        // Edge case: if rank <= 1, return first element
        if (rankAsInt <= 1) {
            return values.get(0);
        }

        // Return value at rank-1 (0-based index)
        return values.get(rankAsInt - 1);
    }

    @Override
    public void reset() {
        values.clear();
    }

    @Override
    public boolean hasData() {
        return !values.isEmpty();
    }
}
