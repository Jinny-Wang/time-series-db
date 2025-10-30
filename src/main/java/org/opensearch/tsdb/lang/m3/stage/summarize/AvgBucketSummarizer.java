/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage.summarize;

/**
 * Bucket summarizer that computes the average of all values.
 *
 * <p>NaN values are excluded from both the sum and count.</p>
 */
public class AvgBucketSummarizer implements BucketSummarizer {
    private double sum;
    private int count;

    public AvgBucketSummarizer() {
        reset();
    }

    @Override
    public void accumulate(double value) {
        if (!Double.isNaN(value)) {
            sum += value;
            count++;
        }
    }

    @Override
    public double finish() {
        return count > 0 ? sum / count : Double.NaN;
    }

    @Override
    public void reset() {
        sum = 0.0;
        count = 0;
    }

    @Override
    public boolean hasData() {
        return count > 0;
    }
}
