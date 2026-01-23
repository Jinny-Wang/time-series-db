/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.translog;

import org.opensearch.telemetry.metrics.tags.Tags;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RateLimitedTranslogDeletionPolicyTests extends OpenSearchTestCase {

    public void testConstructorAndSetter() {
        RateLimitedTranslogDeletionPolicy policy = new RateLimitedTranslogDeletionPolicy(1024L, 3600000L, 10, 50, Tags.EMPTY);
        policy.setMaxTranslogReadersToClosePercentage(75);
    }

    public void testNoRateLimitingWhenPercentageIs100() throws IOException {
        RateLimitedTranslogDeletionPolicy policy = new RateLimitedTranslogDeletionPolicy(1024L, 3600000L, 10, 100, Tags.EMPTY);

        List<TranslogReader> readers = createMockReaders(new long[] { 1, 2, 3, 4, 5 });
        TranslogWriter writer = createMockWriter(6);

        // with percentage=100, should return original min gen (all readers eligible)
        long minGen = policy.minTranslogGenRequired(readers, writer);

        assertEquals(6L, minGen);
    }

    public void testNoRateLimitingWhenReadersIsEmpty() throws IOException {
        RateLimitedTranslogDeletionPolicy policy = new RateLimitedTranslogDeletionPolicy(1024L, 3600000L, 10, 50, Tags.EMPTY);

        List<TranslogReader> readers = new ArrayList<>();
        TranslogWriter writer = createMockWriter(1);

        // with empty readers, should return writer generation
        long minGen = policy.minTranslogGenRequired(readers, writer);

        assertEquals(1L, minGen);
    }

    /**
     * Test that no rate limiting is applied when no readers are eligible for closing.
     */
    public void testNoRateLimitingWhenNoEligibleReaders() throws IOException {
        RateLimitedTranslogDeletionPolicy policy = new RateLimitedTranslogDeletionPolicy(
            Long.MAX_VALUE, // Large retention size - keeps all readers
            Long.MAX_VALUE, // Large retention age - keeps all readers
            100, // Large file count - keeps all readers
            50,
            Tags.EMPTY
        );

        List<TranslogReader> readers = createMockReaders(new long[] { 1, 2, 3, 4, 5 });
        TranslogWriter writer = createMockWriter(6);

        // with large retention, original policy won't close any readers (minGen = 1)
        long minGen = policy.minTranslogGenRequired(readers, writer);

        // should return original minGen (1, as all readers are retained)
        assertEquals(1L, minGen);
    }

    /**
     * Test rate limiting is applied correctly when percentage limits closeable readers.
     */
    public void testRateLimitingApplied() throws IOException {
        // Use zero retention to make all readers eligible for closing
        RateLimitedTranslogDeletionPolicy policy = new RateLimitedTranslogDeletionPolicy(
            0L, // No size retention
            0L, // No age retention
            0, // No file count retention
            50, // 50% rate limit
            Tags.EMPTY
        );

        // Create 10 readers with generations 1-10
        List<TranslogReader> readers = createMockReaders(new long[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 });
        TranslogWriter writer = createMockWriter(11);

        long minGen = policy.minTranslogGenRequired(readers, writer);

        // With 0 retention, all 10 readers are eligible for closing
        // With 50% limit, maxReadersToClose = 5
        // Adjusted minGen = 6
        assertEquals(6L, minGen);
    }

    public void testRateLimitingWith10Percent() throws IOException {
        RateLimitedTranslogDeletionPolicy policy = new RateLimitedTranslogDeletionPolicy(0L, 0L, 0, 10, Tags.EMPTY);

        List<TranslogReader> readers = createMockReaders(new long[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 });
        TranslogWriter writer = createMockWriter(11);

        long minGen = policy.minTranslogGenRequired(readers, writer);
        assertEquals(2L, minGen);
    }

    public void testRateLimitingEnsuresAtLeastOneReaderClosed() throws IOException {
        RateLimitedTranslogDeletionPolicy policy = new RateLimitedTranslogDeletionPolicy(0L, 0L, 0, 1, Tags.EMPTY);

        // With 10 readers, 1% would be 0.1, ensure at least 1 reader will be closed
        List<TranslogReader> readers = createMockReaders(new long[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 });
        TranslogWriter writer = createMockWriter(11);

        long minGen = policy.minTranslogGenRequired(readers, writer);
        assertEquals(2L, minGen);
    }

    /**
     * Test rate limiting with odd number of readers and percentage that doesn't divide evenly.
     */
    public void testRateLimitingWithRounding() throws IOException {
        RateLimitedTranslogDeletionPolicy policy = new RateLimitedTranslogDeletionPolicy(0L, 0L, 0, 33, Tags.EMPTY);

        List<TranslogReader> readers = createMockReaders(new long[] { 1, 2, 3, 4, 5, 6, 7 });
        TranslogWriter writer = createMockWriter(8);

        long minGen = policy.minTranslogGenRequired(readers, writer);

        // With 7 readers and 33%: maxReadersToClose = max(1, 7 * 33 / 100) = max(1, 2) = 2
        // Adjusted minGen = readers.get(2).getGeneration() = 3
        assertEquals(3L, minGen);
    }

    private List<TranslogReader> createMockReaders(long[] generations) {
        List<TranslogReader> readers = new ArrayList<>();
        for (long gen : generations) {
            TranslogReader reader = mock(TranslogReader.class);
            when(reader.getGeneration()).thenReturn(gen);
            readers.add(reader);
        }
        return readers;
    }

    private TranslogWriter createMockWriter(long generation) {
        TranslogWriter writer = mock(TranslogWriter.class);
        when(writer.getGeneration()).thenReturn(generation);
        return writer;
    }
}
