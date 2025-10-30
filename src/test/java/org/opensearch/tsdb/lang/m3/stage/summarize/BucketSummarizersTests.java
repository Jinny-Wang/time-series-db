/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage.summarize;

import org.opensearch.test.OpenSearchTestCase;

import java.util.function.Supplier;

/**
 * Comprehensive tests for all BucketSummarizer implementations.
 * Focuses on edge cases to improve code coverage:
 * - hasData() = false scenarios
 * - finish() with no data
 * - accumulate(NaN)
 * - reset() functionality
 */
public class BucketSummarizersTests extends OpenSearchTestCase {

    /**
     * Generic helper to test a summarizer with setup action, hasData expectation, and expected result.
     */
    private void assertSummarizer(BucketSummarizer summarizer, Runnable setupAction, boolean expectedHasData, double expectedResult) {
        if (setupAction != null) {
            setupAction.run();
        }

        assertEquals(expectedHasData, summarizer.hasData());
        if (Double.isNaN(expectedResult)) {
            assertTrue(Double.isNaN(summarizer.finish()));
        } else {
            assertEquals(expectedResult, summarizer.finish(), 0.0001);
        }
    }

    /**
     * Tests normal operation: accumulate 1.0, 2.0, 3.0 and verify result.
     */
    private void assertNormalOperation(BucketSummarizer summarizer, double expectedResult) {
        assertSummarizer(summarizer, () -> {
            summarizer.accumulate(1.0);
            summarizer.accumulate(2.0);
            summarizer.accumulate(3.0);
        }, true, expectedResult);
    }

    /**
     * Tests no data scenario: verify hasData() is false and finish() returns expected value.
     */
    private void assertNoData(BucketSummarizer summarizer, double expectedResult) {
        assertSummarizer(summarizer, null, false, expectedResult);
    }

    /**
     * Tests only NaN values: verify hasData() is false and finish() returns expected value.
     */
    private void assertOnlyNaN(BucketSummarizer summarizer, double expectedResult) {
        assertSummarizer(summarizer, () -> {
            summarizer.accumulate(Double.NaN);
            summarizer.accumulate(Double.NaN);
        }, false, expectedResult);
    }

    /**
     * Tests mixed with NaN: accumulate valid values mixed with NaN and verify result.
     */
    private void assertMixedWithNaN(BucketSummarizer summarizer, double expectedResult) {
        assertSummarizer(summarizer, () -> {
            summarizer.accumulate(1.0);
            summarizer.accumulate(Double.NaN);
            summarizer.accumulate(2.0);
            summarizer.accumulate(Double.NaN);
            summarizer.accumulate(3.0);
        }, true, expectedResult);
    }

    /**
     * Tests reset functionality: accumulate data, reset, verify cleared state.
     */
    private void assertReset(BucketSummarizer summarizer, double expectedResultAfterReset) {
        assertSummarizer(summarizer, () -> {
            summarizer.accumulate(10.0);
            assertTrue(summarizer.hasData());
            summarizer.reset();
        }, false, expectedResultAfterReset);
    }

    /**
     * Runs all common test patterns for a summarizer type.
     */
    private void testAllPatterns(
        Supplier<BucketSummarizer> supplier,
        double normalOp,
        double noData,
        double onlyNaN,
        double mixedNaN,
        double reset
    ) {
        assertNormalOperation(supplier.get(), normalOp);
        assertNoData(supplier.get(), noData);
        assertOnlyNaN(supplier.get(), onlyNaN);
        assertMixedWithNaN(supplier.get(), mixedNaN);
        assertReset(supplier.get(), reset);
    }

    public void testSumBucketSummarizer() {
        testAllPatterns(SumBucketSummarizer::new, 6.0, 0.0, 0.0, 6.0, 0.0);
    }

    public void testAvgBucketSummarizer() {
        testAllPatterns(AvgBucketSummarizer::new, 2.0, Double.NaN, Double.NaN, 2.0, Double.NaN);
    }

    public void testMaxBucketSummarizer() {
        testAllPatterns(MaxBucketSummarizer::new, 3.0, Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY, 3.0, Double.NEGATIVE_INFINITY);
    }

    public void testMinBucketSummarizer() {
        testAllPatterns(MinBucketSummarizer::new, 1.0, Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY, 1.0, Double.POSITIVE_INFINITY);
    }

    public void testLastBucketSummarizer() {
        testAllPatterns(LastBucketSummarizer::new, 3.0, Double.NaN, Double.NaN, 3.0, Double.NaN);
    }

    public void testStdDevBucketSummarizer_normalOperation() {
        StdDevBucketSummarizer summarizer = new StdDevBucketSummarizer();
        // Use a custom test for StdDev as it needs different values than 1,2,3
        summarizer.accumulate(2.0);
        summarizer.accumulate(4.0);
        summarizer.accumulate(4.0);
        summarizer.accumulate(4.0);
        summarizer.accumulate(5.0);
        summarizer.accumulate(5.0);
        summarizer.accumulate(7.0);
        summarizer.accumulate(9.0);

        assertTrue(summarizer.hasData());
        // Expected sample stddev for this dataset [2,4,4,4,5,5,7,9]: mean=5, variance=32/7≈4.571, stddev≈2.138
        assertEquals(2.138, summarizer.finish(), 0.01);
    }

    public void testStdDevBucketSummarizer_singleValue() {
        StdDevBucketSummarizer summarizer = new StdDevBucketSummarizer();

        summarizer.accumulate(5.0);

        assertTrue(summarizer.hasData());
        assertEquals(0.0, summarizer.finish(), 0.0001);
    }

    public void testStdDevBucketSummarizer_noData() {
        assertNoData(new StdDevBucketSummarizer(), Double.NaN);
    }

    public void testStdDevBucketSummarizer_onlyNaN() {
        assertOnlyNaN(new StdDevBucketSummarizer(), Double.NaN);
    }

    public void testStdDevBucketSummarizer_mixedWithNaN() {
        // Custom test for StdDev as the expected result differs from standard test
        StdDevBucketSummarizer summarizer = new StdDevBucketSummarizer();
        summarizer.accumulate(Double.NaN);
        summarizer.accumulate(2.0);
        summarizer.accumulate(Double.NaN);
        summarizer.accumulate(4.0);
        summarizer.accumulate(Double.NaN);
        summarizer.accumulate(6.0);

        assertTrue(summarizer.hasData());
        // Expected sample stddev for [2, 4, 6] is 2.0
        assertEquals(2.0, summarizer.finish(), 0.0001);
    }

    public void testStdDevBucketSummarizer_reset() {
        assertReset(new StdDevBucketSummarizer(), Double.NaN);
    }

    public void testPercentileBucketSummarizer_normalOperation() {
        PercentileBucketSummarizer summarizer = new PercentileBucketSummarizer(50); // p50 (median)

        summarizer.accumulate(1.0);
        summarizer.accumulate(2.0);
        summarizer.accumulate(3.0);
        summarizer.accumulate(4.0);
        summarizer.accumulate(5.0);

        assertTrue(summarizer.hasData());
        assertEquals(3.0, summarizer.finish(), 0.0001);
    }

    public void testPercentileBucketSummarizer_p95() {
        PercentileBucketSummarizer summarizer = new PercentileBucketSummarizer(95);

        for (int i = 1; i <= 100; i++) {
            summarizer.accumulate(i);
        }

        assertTrue(summarizer.hasData());
        // p95 of 1-100 should be 95
        assertEquals(95.0, summarizer.finish(), 0.0001);
    }

    public void testPercentileBucketSummarizer_p0() {
        PercentileBucketSummarizer summarizer = new PercentileBucketSummarizer(0);

        summarizer.accumulate(5.0);
        summarizer.accumulate(1.0);
        summarizer.accumulate(3.0);

        assertTrue(summarizer.hasData());
        assertEquals(1.0, summarizer.finish(), 0.0001);
    }

    public void testPercentileBucketSummarizer_p100() {
        PercentileBucketSummarizer summarizer = new PercentileBucketSummarizer(100);

        summarizer.accumulate(5.0);
        summarizer.accumulate(1.0);
        summarizer.accumulate(3.0);

        assertTrue(summarizer.hasData());
        assertEquals(5.0, summarizer.finish(), 0.0001);
    }

    public void testPercentileBucketSummarizer_noData() {
        assertNoData(new PercentileBucketSummarizer(50), Double.NaN);
    }

    public void testPercentileBucketSummarizer_onlyNaN() {
        assertOnlyNaN(new PercentileBucketSummarizer(50), Double.NaN);
    }

    public void testPercentileBucketSummarizer_mixedWithNaN() {
        assertMixedWithNaN(new PercentileBucketSummarizer(50), 2.0);
    }

    public void testPercentileBucketSummarizer_reset() {
        assertReset(new PercentileBucketSummarizer(50), Double.NaN);
    }

    public void testPercentileBucketSummarizer_invalidPercentile() {
        assertThrows(IllegalArgumentException.class, () -> new PercentileBucketSummarizer(-1));
        assertThrows(IllegalArgumentException.class, () -> new PercentileBucketSummarizer(101));
    }

    public void testPercentileBucketSummarizer_singleValue() {
        PercentileBucketSummarizer summarizer = new PercentileBucketSummarizer(50);

        summarizer.accumulate(42.0);

        assertTrue(summarizer.hasData());
        assertEquals(42.0, summarizer.finish(), 0.0001);
    }
}
