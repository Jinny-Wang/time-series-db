/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage.summarize;

/**
 * Bucket summarizer that computes the maximum value.
 *
 * <p>NaN values are skipped and do not affect the maximum.</p>
 */
public class MaxBucketSummarizer implements BucketSummarizer {
    private double max;
    private boolean hasValue;

    public MaxBucketSummarizer() {
        reset();
    }

    @Override
    public void accumulate(double value) {
        if (!Double.isNaN(value)) {
            if (!hasValue || value > max) {
                max = value;
            }
            hasValue = true;
        }
    }

    @Override
    public double finish() {
        return max;
    }

    @Override
    public void reset() {
        max = Double.NEGATIVE_INFINITY;
        hasValue = false;
    }

    @Override
    public boolean hasData() {
        return hasValue;
    }
}
