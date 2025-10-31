/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.plan.nodes;

import org.opensearch.tsdb.lang.m3.common.Constants;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.FunctionNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.ValueNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.M3PlannerContext;
import org.opensearch.tsdb.lang.m3.m3ql.plan.visitor.M3PlanVisitor;

import java.util.Locale;
import java.util.Set;

/**
 * SortPlanNode represents a plan node that handles sort operations in M3QL.
 *
 * The sort function sorts time series by either avg, max, or sum of their values.
 * It takes an optional second argument for sort order (asc or desc, defaulting to desc).
 *
 * This is a global aggregation that can only be executed at the coordinator aggregator,
 * similar to histogramPercentile.
 */
public class SortPlanNode extends M3PlanNode {
    private static final String ASC_SORT_ORDER = "asc";
    private static final String DESC_SORT_ORDER = "desc";
    private static final String DEFAULT_SORT_ORDER = DESC_SORT_ORDER;
    private static final Set<String> validSortBy = Set.of(
        Constants.Functions.Sort.AVG,
        Constants.Functions.Sort.CURRENT,
        Constants.Functions.Sort.MAX,
        Constants.Functions.Sort.STD_DEV,
        Constants.Functions.Sort.SUM
    );

    private final String sortBy;
    private final String sortOrder;

    /**
     * Constructor for SortPlanNode.
     *
     * @param id        The node ID
     * @param sortBy    The sorting function (avg, max, sum)
     * @param sortOrder The sorting order (asc, desc)
     */
    public SortPlanNode(int id, String sortBy, String sortOrder) {
        super(id);
        this.sortBy = sortBy;
        this.sortOrder = sortOrder;
    }

    @Override
    public <T> T accept(M3PlanVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String getExplainName() {
        return String.format(Locale.ROOT, "SORT(%s, %s)", sortBy, sortOrder);
    }

    /**
     * Returns the sorting function.
     *
     * @return The sorting function (avg, max, sum)
     */
    public String getSortBy() {
        return sortBy;
    }

    /**
     * Returns the sorting order.
     *
     * @return The sorting order (asc, desc)
     */
    public String getSortOrder() {
        return sortOrder;
    }

    /**
     * Creates a SortPlanNode from a FunctionNode.
     * Expected format:
     * - sort(avg) -> defaults to desc
     * - sort(avg, desc)
     * - sort(max, asc)
     * - sort(sum)
     *
     * @param functionNode The function node to parse
     * @return SortPlanNode instance
     * @throws IllegalArgumentException if the function arguments are invalid
     */
    public static SortPlanNode of(FunctionNode functionNode) {
        String sortBy = getSortBy(functionNode);

        // Second argument (optional): sort order
        String sortOrder = DEFAULT_SORT_ORDER; // Default
        if (functionNode.getChildren().size() > 1) {
            if (!(functionNode.getChildren().get(1) instanceof ValueNode sortOrderValue)) {
                throw new IllegalArgumentException("Sort order argument must be a value (asc, desc)");
            }
            sortOrder = sortOrderValue.getValue();

            // Validate sort order
            if (!ASC_SORT_ORDER.equals(sortOrder) && !DESC_SORT_ORDER.equals(sortOrder)) {
                throw new IllegalArgumentException("Sort order must be 'asc' or 'desc'. Got: " + sortOrder);
            }
        }

        if (functionNode.getChildren().size() > 2) {
            throw new IllegalArgumentException("sort function accepts at most 2 arguments: sort function and order");
        }

        return new SortPlanNode(M3PlannerContext.generateId(), sortBy, sortOrder);
    }

    private static String getSortBy(FunctionNode functionNode) {
        if (functionNode.getChildren().isEmpty()) {
            throw new IllegalArgumentException("sort function must have at least one argument: sort function (avg, max, sum)");
        }

        // First argument: sort function
        if (!(functionNode.getChildren().getFirst() instanceof ValueNode valueNode)) {
            throw new IllegalArgumentException("Sort function argument must be a value (avg, max, sum)");
        }
        String sortBy = valueNode.getValue();

        // Validate sort function
        if (!validSortBy.contains(sortBy)) {
            throw new IllegalArgumentException("Sort function must be one of: avg, current, max, stddev, sum. Got: " + sortBy);
        }
        return sortBy;
    }
}
