/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage.summarize;

/**
 * Bucket summarizer that returns the last (most recent) value.
 *
 * <p>NaN values are skipped. The last non-NaN value is returned.</p>
 */
public class LastBucketSummarizer implements BucketSummarizer {
    private double last;
    private boolean hasValue;

    public LastBucketSummarizer() {
        reset();
    }

    @Override
    public void accumulate(double value) {
        if (!Double.isNaN(value)) {
            last = value;
            hasValue = true;
        }
    }

    @Override
    public double finish() {
        return last;
    }

    @Override
    public void reset() {
        last = Double.NaN;
        hasValue = false;
    }

    @Override
    public boolean hasData() {
        return hasValue;
    }
}
