/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.index.closed;

import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.util.BytesRef;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.core.chunk.Chunk;
import org.opensearch.tsdb.core.chunk.ChunkAppender;
import org.opensearch.tsdb.core.chunk.ChunkIterator;
import org.opensearch.tsdb.core.chunk.XORChunk;
import org.opensearch.tsdb.core.chunk.XORIterator;

public class ClosedChunkIndexIOTests extends OpenSearchTestCase {

    public void testChunkSerDe() {
        Chunk chunk = new XORChunk();
        ChunkAppender appender = chunk.appender();
        for (int i = 0; i < 10; i++) {
            appender.append(i, i * 10);
        }

        BytesRef ref = ClosedChunkIndexIO.serializeChunk(chunk);
        ClosedChunk deserializedChunk = ClosedChunkIndexIO.getClosedChunkFromSerialized(ref);

        ChunkIterator memChunkIterator = chunk.iterator();
        ChunkIterator deserializedIterator = deserializedChunk.getChunkIterator();

        assertTrue(deserializedIterator instanceof XORIterator);

        while (memChunkIterator.next() != ChunkIterator.ValueType.NONE && deserializedIterator.next() != ChunkIterator.ValueType.NONE) {
            assertEquals(memChunkIterator.at().timestamp(), deserializedIterator.at().timestamp());
            assertEquals(memChunkIterator.at().value(), deserializedIterator.at().value(), 0.0001);
        }

        assertEquals(ChunkIterator.ValueType.NONE, memChunkIterator.next());
        assertEquals(ChunkIterator.ValueType.NONE, deserializedIterator.next());
    }

    public void testUnsupportedVersion() {
        int version = 99;
        byte[] bytes = new byte[10];
        ByteArrayDataOutput out = new ByteArrayDataOutput(bytes);
        out.writeByte((byte) version); // first byte is version
        BytesRef ref = new BytesRef(bytes, 0, out.getPosition());

        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> { ClosedChunkIndexIO.getClosedChunkFromSerialized(ref); }
        );
        assertEquals("Unsupported chunk version: 99", e.getMessage());
    }
}
