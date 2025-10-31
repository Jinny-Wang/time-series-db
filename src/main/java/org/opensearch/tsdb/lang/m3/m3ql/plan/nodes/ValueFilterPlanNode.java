/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.plan.nodes;

import org.opensearch.tsdb.lang.m3.common.ValueFilterType;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.FunctionNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.M3ASTNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.ValueNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.M3PlannerContext;
import org.opensearch.tsdb.lang.m3.m3ql.plan.visitor.M3PlanVisitor;

import java.util.Locale;

/**
 * ValueFilterPlanNode represents a node in the M3QL plan that filters values based on some {@link ValueFilterType}.
 */
public class ValueFilterPlanNode extends M3PlanNode {
    private final ValueFilterType filter;
    private final double targetValue;

    /**
     * Constructor for ValueFilterPlanNode.
     *
     * @param id          node id
     * @param filter      filter the type of filter to apply
     * @param targetValue the value to filter on
     */
    public ValueFilterPlanNode(int id, ValueFilterType filter, double targetValue) {
        super(id);
        this.filter = filter;
        this.targetValue = targetValue;
    }

    @Override
    public <T> T accept(M3PlanVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String getExplainName() {
        return String.format(Locale.ROOT, "VALUE_FILTER(%s, %s)", filter.name(), targetValue);
    }

    /**
     * Get the target value for value filter comparison.
     * @return the target value
     */
    public double getTargetValue() {
        return targetValue;
    }

    /**
     * Get the filter type for value filtering.
     * @return the filter type
     */
    public ValueFilterType getFilter() {
        return filter;
    }

    /**
     * Create an ValueFilterPlanNode from a FunctionNode AST node.
     *
     * @param functionNode The function node representing the value filter function call
     * @return A new ValueFilterPlanNode instance
     * @throws IllegalArgumentException if the function node is invalid
     */
    public static ValueFilterPlanNode of(FunctionNode functionNode) {
        ValueFilterType filter = ValueFilterType.fromString(functionNode.getFunctionName());

        String valueStr = getValueString(functionNode);
        try {
            double value = Double.parseDouble(valueStr);
            return new ValueFilterPlanNode(M3PlannerContext.generateId(), filter, value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid numeric value for " + functionNode.getFunctionName() + " function: " + valueStr, e);
        }
    }

    private static String getValueString(FunctionNode functionNode) {
        if (functionNode.getChildren().size() != 1) {
            throw new IllegalArgumentException(
                functionNode.getFunctionName() + " function expects exactly one argument, but got " + functionNode.getChildren().size()
            );
        }

        M3ASTNode child = functionNode.getChildren().getFirst();
        if (!(child instanceof ValueNode valueNode)) {
            throw new IllegalArgumentException(
                "Argument to " + functionNode.getFunctionName() + " should be a value node, but got " + child.getClass().getSimpleName()
            );
        }

        return valueNode.getValue();
    }
}
