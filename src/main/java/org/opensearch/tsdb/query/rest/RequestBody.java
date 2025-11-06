/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.rest;

import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;

/**
 * Represents the request body for M3QL queries.
 *
 * @param query the M3QL query string
 * @param resolvedPartitions optional partition resolution information for federation
 */
public record RequestBody(String query, ResolvedPartitions resolvedPartitions) {
    private static final String FIELD_QUERY = "query";
    private static final String FIELD_RESOLVED_PARTITIONS = "resolved_partitions";

    /**
     * Parses RequestBody from XContent.
     *
     * @param parser the XContent parser
     * @return parsed RequestBody
     * @throws IOException if parsing fails
     */
    public static RequestBody parse(XContentParser parser) throws IOException {
        String query = null;
        ResolvedPartitions resolvedPartitions = null;

        XContentParser.Token token;
        String currentFieldName = null;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_STRING && FIELD_QUERY.equals(currentFieldName)) {
                query = parser.text();
            } else if (token == XContentParser.Token.START_OBJECT && FIELD_RESOLVED_PARTITIONS.equals(currentFieldName)) {
                resolvedPartitions = ResolvedPartitions.parse(parser);
            }
        }

        return new RequestBody(query, resolvedPartitions);
    }
}
