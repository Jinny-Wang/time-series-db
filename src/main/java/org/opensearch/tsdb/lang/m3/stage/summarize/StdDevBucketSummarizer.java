/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage.summarize;

import java.util.ArrayList;
import java.util.List;

/**
 * Bucket summarizer that computes the standard deviation of all values.
 *
 * <p>This is a value-based summarizer that stores all values in the bucket
 * and computes the standard deviation in the finish() method. NaN values are excluded.</p>
 *
 * <p>Uses the sample standard deviation formula:
 * stddev = sqrt(sum((x - mean)^2) / (n - 1))</p>
 *
 * <p>For a single value, returns 0 (no variation).</p>
 */
public class StdDevBucketSummarizer implements BucketSummarizer {
    private final List<Double> values;

    public StdDevBucketSummarizer() {
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
        int n = values.size();

        if (n == 0) {
            return Double.NaN;
        }

        if (n == 1) {
            // Single value has no variation
            return 0.0;
        }

        // Calculate mean
        double sum = 0.0;
        for (double value : values) {
            sum += value;
        }
        double mean = sum / n;

        // Calculate sum of squared differences from mean
        double sumSquaredDiff = 0.0;
        for (double value : values) {
            double diff = value - mean;
            sumSquaredDiff += diff * diff;
        }

        // Sample standard deviation: divide by (n-1)
        double variance = sumSquaredDiff / (n - 1);
        return Math.sqrt(variance);
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
