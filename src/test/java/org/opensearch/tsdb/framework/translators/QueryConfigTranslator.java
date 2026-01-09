/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.framework.translators;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.tsdb.framework.models.QueryConfig;

/**
 * Interface for translating different query types to OpenSearch SearchRequest.
 * This translator can be used in both REST tests and internal cluster tests.
 *
 * <p>Supports Cross-Cluster Search (CCS) index patterns. When indices contain
 * cluster prefixes (e.g., "cluster_a:metrics,cluster_b:metrics"), the translator
 * handles them appropriately.
 */
public interface QueryConfigTranslator {

    /**
     * Translate a query config to OpenSearch SearchRequest.
     *
     * <p>The indices parameter can be:
     * <ul>
     *   <li>A single index: "metrics"</li>
     *   <li>Multiple indices: "metrics,logs"</li>
     *   <li>CCS pattern: "cluster_a:metrics,cluster_b:metrics"</li>
     *   <li>Mixed pattern: "local_metrics,cluster_a:remote_metrics"</li>
     * </ul>
     *
     * @param queryConfig The query config to translate
     * @param indices The target indices (comma-separated, may include cluster prefixes)
     * @return SearchRequest for OpenSearch
     * @throws Exception if translation fails
     */
    SearchRequest translate(QueryConfig queryConfig, String indices) throws Exception;

    /**
     * Parse comma-separated indices string into array.
     *
     * @param indices comma-separated indices string
     * @return array of index patterns
     */
    default String[] parseIndices(String indices) {
        if (indices == null || indices.isBlank()) {
            throw new IllegalArgumentException("Indices cannot be null or empty");
        }
        return indices.split(",");
    }
}
