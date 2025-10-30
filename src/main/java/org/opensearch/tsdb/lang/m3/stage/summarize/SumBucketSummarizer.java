/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage.summarize;

/**
 * Bucket summarizer that computes the sum of all values.
 *
 * <p>NaN values are skipped and do not contribute to the sum.</p>
 */
public class SumBucketSummarizer implements BucketSummarizer {
    private double sum;
    private int count;

    public SumBucketSummarizer() {
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
        return sum;
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
