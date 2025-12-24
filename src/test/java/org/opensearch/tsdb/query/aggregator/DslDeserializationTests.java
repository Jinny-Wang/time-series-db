/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.aggregator;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.plugins.SearchPlugin;
import org.opensearch.search.SearchModule;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.PipelineAggregationBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.TSDBPlugin;
import org.opensearch.tsdb.TestUtils;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;

/**
 * Tests that all .dsl fixture files can be successfully deserialized to SearchSourceBuilder.
 *
 * <p>This test ensures that:
 * <ul>
 *   <li>All .dsl files in test resources can be parsed without exceptions</li>
 *   <li>Custom aggregation builders (TimeSeriesUnfoldAggregationBuilder, TimeSeriesCoordinatorAggregationBuilder)
 *       properly implement their parse() methods</li>
 *   <li>All pipeline stages can be deserialized via PipelineStageFactory</li>
 *   <li>Boolean, number, and array field types are correctly handled</li>
 *   <li>Custom query builders (TimeRangePruningQueryBuilder) can be deserialized</li>
 * </ul>
 *
 * <p>Each .dsl file runs as a separate parameterized test case for clear failure reporting.</p>
 */
public class DslDeserializationTests extends OpenSearchTestCase {

    private final String testCaseName;
    private final String dslContent;
    private static NamedXContentRegistry xContentRegistry;

    /**
     * Constructor for parameterized test.
     *
     * @param testCaseName Name of the test case (file name)
     * @param dslContent Content of the .dsl file
     */
    public DslDeserializationTests(String testCaseName, String dslContent) {
        this.testCaseName = testCaseName;
        this.dslContent = dslContent;
    }

    /**
     * Set up NamedXContentRegistry with TSDBPlugin registrations.
     * This is called once before all tests to initialize the registry with all custom parsers.
     */
    @Override
    public void setUp() throws Exception {
        super.setUp();
        if (xContentRegistry == null) {
            // Create SearchModule with TSDBPlugin to get all registered parsers
            List<SearchPlugin> plugins = List.of(new TSDBPlugin());
            SearchModule searchModule = new SearchModule(Settings.EMPTY, plugins);
            xContentRegistry = new NamedXContentRegistry(searchModule.getNamedXContents());
        }
    }

    /**
     * Parameters factory that loads all .dsl files from test resources.
     *
     * @return Iterable of test parameters (testCaseName, dslContent)
     */
    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        try {
            List<Object[]> testCases = new ArrayList<>();

            // Load all .dsl files from test resources
            NavigableMap<String, String> dslFiles = TestUtils.getResourceFilesWithExtension("lang/m3/data/dsl", ".dsl");

            for (var entry : dslFiles.entrySet()) {
                String testCaseName = entry.getKey();
                String dslContent = entry.getValue().trim(); // Remove trailing newline
                testCases.add(new Object[] { testCaseName, dslContent });
            }

            return testCases;
        } catch (IOException | URISyntaxException e) {
            throw new RuntimeException("Failed to load .dsl test data: " + e.getMessage(), e);
        }
    }

    /**
     * Main test method that deserializes a .dsl file to SearchSourceBuilder and validates no errors occur.
     */
    public void testDslDeserialization() {
        try {
            // Parse the DSL JSON content to SearchSourceBuilder
            SearchSourceBuilder searchSourceBuilder = parseSearchSourceBuilder(dslContent);

            // Verify basic structure is valid
            assertNotNull("SearchSourceBuilder should not be null for test case: " + testCaseName, searchSourceBuilder);

            // Verify aggregations are present (all our .dsl files should have aggregations)
            if (searchSourceBuilder.aggregations() != null) {
                assertNotNull(
                    "Aggregations should not be null when present for test case: " + testCaseName,
                    searchSourceBuilder.aggregations().getAggregatorFactories()
                );

                // Verify each aggregation was deserialized successfully
                for (AggregationBuilder aggBuilder : searchSourceBuilder.aggregations().getAggregatorFactories()) {
                    assertNotNull("Aggregation builder should not be null for test case: " + testCaseName, aggBuilder);
                    assertNotNull("Aggregation name should not be null for test case: " + testCaseName, aggBuilder.getName());

                    // Additional validation for our custom aggregation builders
                    if (aggBuilder instanceof TimeSeriesUnfoldAggregationBuilder) {
                        TimeSeriesUnfoldAggregationBuilder unfoldBuilder = (TimeSeriesUnfoldAggregationBuilder) aggBuilder;
                        assertTrue(
                            "TimeSeriesUnfoldAggregationBuilder should have maxTimestamp > minTimestamp for test case: " + testCaseName,
                            unfoldBuilder.getMaxTimestamp() > unfoldBuilder.getMinTimestamp()
                        );
                        assertTrue(
                            "TimeSeriesUnfoldAggregationBuilder should have step > 0 for test case: " + testCaseName,
                            unfoldBuilder.getStep() > 0
                        );
                    }
                }

                // Verify pipeline aggregations if present
                for (PipelineAggregationBuilder pipelineAggBuilder : searchSourceBuilder.aggregations().getPipelineAggregatorFactories()) {
                    assertNotNull("Pipeline aggregation builder should not be null for test case: " + testCaseName, pipelineAggBuilder);
                    assertNotNull(
                        "Pipeline aggregation name should not be null for test case: " + testCaseName,
                        pipelineAggBuilder.getName()
                    );

                    // Additional validation for our custom pipeline aggregation builders
                    if (pipelineAggBuilder instanceof TimeSeriesCoordinatorAggregationBuilder) {
                        TimeSeriesCoordinatorAggregationBuilder coordBuilder = (TimeSeriesCoordinatorAggregationBuilder) pipelineAggBuilder;
                        assertNotNull(
                            "TimeSeriesCoordinatorAggregationBuilder should have stages for test case: " + testCaseName,
                            coordBuilder.getStages()
                        );
                        assertFalse(
                            "TimeSeriesCoordinatorAggregationBuilder stages should not be empty for test case: " + testCaseName,
                            coordBuilder.getStages().isEmpty()
                        );
                    }
                }
            }

            // Verify query is present (all our .dsl files should have queries)
            assertNotNull("Query should not be null for test case: " + testCaseName, searchSourceBuilder.query());

        } catch (Exception e) {
            fail("Failed to deserialize .dsl file '" + testCaseName + "': " + e.getMessage() + "\n" + getStackTraceAsString(e));
        }
    }

    /**
     * Parse DSL JSON content to SearchSourceBuilder using the NamedXContentRegistry.
     *
     * @param dslJson The DSL JSON content
     * @return Parsed SearchSourceBuilder
     * @throws IOException If parsing fails
     */
    private SearchSourceBuilder parseSearchSourceBuilder(String dslJson) throws IOException {
        try (
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                xContentRegistry,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                dslJson
            )
        ) {
            return SearchSourceBuilder.fromXContent(parser);
        }
    }

    /**
     * Helper method to get stack trace as string for better error messages.
     *
     * @param e The exception
     * @return Stack trace as string
     */
    private String getStackTraceAsString(Exception e) {
        StringBuilder sb = new StringBuilder();
        sb.append(e.getClass().getName()).append(": ").append(e.getMessage()).append("\n");
        for (StackTraceElement element : e.getStackTrace()) {
            sb.append("  at ").append(element.toString()).append("\n");
        }
        if (e.getCause() != null) {
            sb.append("Caused by: ");
            sb.append(getStackTraceAsString((Exception) e.getCause()));
        }
        return sb.toString();
    }
}
