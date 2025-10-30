/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage.summarize;

/**
 * Bucket summarizer that computes the minimum value.
 *
 * <p>NaN values are skipped and do not affect the minimum.</p>
 */
public class MinBucketSummarizer implements BucketSummarizer {
    private double min;
    private boolean hasValue;

    public MinBucketSummarizer() {
        reset();
    }

    @Override
    public void accumulate(double value) {
        if (!Double.isNaN(value)) {
            if (!hasValue || value < min) {
                min = value;
            }
            hasValue = true;
        }
    }

    @Override
    public double finish() {
        return min;
    }

    @Override
    public void reset() {
        min = Double.POSITIVE_INFINITY;
        hasValue = false;
    }

    @Override
    public boolean hasData() {
        return hasValue;
    }
}
