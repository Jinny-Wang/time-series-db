/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.index.closed;

import org.opensearch.tsdb.core.chunk.ChunkIterator;
import org.opensearch.tsdb.core.chunk.Encoding;
import org.opensearch.tsdb.core.chunk.XORIterator;

/**
 * Implementation of a completed (closed) head chunk.
 *
 * ClosedChunk provides read-only access to compressed chunk data, as well as access to chunk metadata without requiring decompression.
 */
public class ClosedChunk {
    private final ChunkIterator chunkIterator;
    private final Encoding encoding;

    /**
     * Constructs a ClosedChunk with the given parameters.
     * @param bytes the compressed chunk data
     * @param encoding the encoding format used for the chunk data
     */
    public ClosedChunk(byte[] bytes, Encoding encoding) {
        this.chunkIterator = switch (encoding) {
            case XOR -> new XORIterator(bytes);
        };
        this.encoding = encoding;
    }

    /**
     * Returns an iterator over the samples in the chunk.
     * @return the ChunkIterator
     */
    public ChunkIterator getChunkIterator() {
        return chunkIterator;
    }

    /**
     * Returns the encoding format used for the chunk data.
     * @return the Encoding
     */
    public Encoding getEncoding() {
        return encoding;
    }
}
