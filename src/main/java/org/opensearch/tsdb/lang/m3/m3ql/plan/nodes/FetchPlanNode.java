/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.plan.nodes;

import org.opensearch.tsdb.lang.m3.common.Utils;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.TagArgsNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.FunctionNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.M3ASTNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.TagKeyNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.TagValueNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.M3PlannerContext;
import org.opensearch.tsdb.lang.m3.m3ql.plan.visitor.M3PlanVisitor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Represents a fetch plan node in the M3QL query plan.
 */
public class FetchPlanNode extends M3PlanNode {
    private final Map<String, List<String>> matchFilters;
    private final Map<String, List<String>> inverseMatchFilters;

    /**
     * Constructor for FetchPlanNode.
     *
     * @param id node id
     * @param matchFilters Map of field names to lists of filter values for matching
     * @param inverseMatchFilters Map of field names to lists of filter values for inverse matching
     */
    public FetchPlanNode(int id, Map<String, List<String>> matchFilters, Map<String, List<String>> inverseMatchFilters) {
        super(id);
        this.matchFilters = matchFilters;
        this.inverseMatchFilters = inverseMatchFilters;
    }

    @Override
    public <T> T accept(M3PlanVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String getExplainName() {
        return String.format(Locale.ROOT, "FETCH(%s, !%s)", matchFilters.toString(), inverseMatchFilters.toString());
    }

    /**
     * Returns match filters for the fetch
     * @return a map where keys are field names and values are lists of filter values
     */
    public Map<String, List<String>> getMatchFilters() {
        return matchFilters;
    }

    /**
     * Returns inverse match filters for the fetch
     * @return a map where keys are field names and values are lists of filter values
     */
    public Map<String, List<String>> getInverseMatchFilters() {
        return inverseMatchFilters;
    }

    /**
     * Creates the fetch plan node from the corresponding AST Node
     * @param functionNode The function AST node representing the fetch
     * @return FetchPlanNode instance
     */
    public static FetchPlanNode of(FunctionNode functionNode) {
        // Generate filters
        if (functionNode == null) {
            throw new IllegalArgumentException("FunctionNode cannot be null");
        }

        Map<String, List<String>> matchFilters = new HashMap<>();
        Map<String, List<String>> inverseMatchFilters = new HashMap<>();
        for (M3ASTNode m3ASTNode : functionNode.getChildren()) {
            if (!(m3ASTNode instanceof TagKeyNode tagKey)) {
                throw new IllegalArgumentException("Expected TagKeyNode, but found: " + m3ASTNode.getClass().getSimpleName());
            }

            String keyName = tagKey.getKeyName();
            if (keyName == null || keyName.isEmpty()) {
                throw new IllegalArgumentException("Key name cannot be empty in label specifiers");
            }

            if (tagKey.isInverted()) {
                inverseMatchFilters.put(keyName, getFilterValuesFromLabelKey(tagKey));
            } else {
                matchFilters.put(keyName, getFilterValuesFromLabelKey(tagKey));
            }
        }

        return new FetchPlanNode(M3PlannerContext.generateId(), matchFilters, inverseMatchFilters);
    }

    private static List<String> getFilterValuesFromLabelKey(TagKeyNode tagKey) {
        if (tagKey.getChildren().isEmpty()) {
            throw new IllegalArgumentException("TagKeyNode must have at least one child");
        }
        if (tagKey.getChildren().size() > 1) {
            throw new IllegalArgumentException("TagKeyNode can only have one child for filter values");
        }
        M3ASTNode child = tagKey.getChildren().getFirst();
        if (child instanceof TagValueNode tagValueNode) {
            return Collections.singletonList(Utils.stripDoubleQuotes(tagValueNode.getValue())); // Single value case
        }
        if (child instanceof TagArgsNode argsNode) {
            List<String> sanitizedArgs = new ArrayList<>();
            for (String arg : argsNode.getArgs()) {
                sanitizedArgs.add(Utils.stripDoubleQuotes(arg));
            }
            return sanitizedArgs;
        }
        throw new IllegalArgumentException("Invalid filter value for label, got " + child.getClass().getSimpleName());
    }
}
