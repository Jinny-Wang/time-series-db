/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage;

import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.tsdb.query.stage.PipelineStage;

import java.io.IOException;
import java.util.Map;

/**
 * Utility class for testing pipeline stages.
 * Provides helper methods for serialization and deserialization testing.
 */
public class PipelineStageTestUtils {
    /**
     * Serialize a pipeline stage to JSON and parse back to Map&lt;String, Object&gt;.
     * This simulates the round-trip through JSON that happens in production when
     * stages are serialized via toXContent and deserialized via fromArgs.
     *
     * @param stage The pipeline stage to serialize
     * @return Map of field names to values extracted from JSON
     * @throws IOException if serialization or parsing fails
     */
    public static Map<String, Object> serializeToArgs(PipelineStage stage) throws IOException {
        // Serialize to JSON using toXContent
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        stage.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        String json = builder.toString();

        // Parse JSON back to Map (simulating fromArgs input)
        try (
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                org.opensearch.core.xcontent.NamedXContentRegistry.EMPTY,
                org.opensearch.core.xcontent.DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                json
            )
        ) {
            parser.nextToken(); // Move to START_OBJECT
            return parser.map();
        }
    }
}
