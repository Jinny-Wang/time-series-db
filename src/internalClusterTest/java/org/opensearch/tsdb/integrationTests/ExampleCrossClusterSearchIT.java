/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.integrationTests;

import org.opensearch.tsdb.framework.MultiClusterTimeSeriesTestFramework;

/**
 * Example Cross-Cluster Search (CCS) integration test.
 *
 * <p>This test demonstrates the multi-cluster TSDB test framework capabilities:
 * <ul>
 *   <li>Creating remote clusters with YAML configuration</li>
 *   <li>Ingesting data into specific clusters</li>
 *   <li>Executing CCS queries across multiple clusters</li>
 *   <li>Validating query results from distributed data</li>
 * </ul>
 *
 * <p>The test uses the {@link MultiClusterTimeSeriesTestFramework} which extends
 * OpenSearch's AbstractMultiClustersTestCase for proper multi-cluster lifecycle management.
 *
 * <h2>Usage</h2>
 * <pre>{@code
 * ./gradlew internalClusterTest --tests "ExampleCrossClusterSearchIT"
 * }</pre>
 *
 * @see MultiClusterTimeSeriesTestFramework
 */
public class ExampleCrossClusterSearchIT extends MultiClusterTimeSeriesTestFramework {

    /**
     * Returns the path to the YAML test configuration file.
     */
    @Override
    protected String getTestConfigurationPath() {
        return "test_cases/example_cross_cluster_search_it.yaml";
    }

    /**
     * Test cross-cluster search with data distributed across local and remote clusters.
     *
     * <p>This test:
     * <ol>
     *   <li>Creates indices on local cluster and two remote clusters</li>
     *   <li>Ingests region-specific HTTP metrics into each cluster</li>
     *   <li>Executes M3QL queries targeting single remote cluster</li>
     *   <li>Executes CCS queries targeting multiple remote clusters</li>
     *   <li>Executes mixed queries targeting both local and remote clusters</li>
     * </ol>
     *
     * @throws Exception if test execution fails
     */
    public void testCrossClusterSearch() throws Exception {
        runBasicTest();
    }
}
