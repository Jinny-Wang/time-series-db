/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.plan.nodes;

import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.FunctionNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.ValueNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.M3PlannerContext;
import org.opensearch.tsdb.lang.m3.m3ql.plan.visitor.M3PlanVisitor;

import java.util.Locale;

/**
 * TransformNullPlanNode represents a plan node that transforms null values in the data.
 */
public class TransformNullPlanNode extends M3PlanNode {

    private static final Double DEFAULT_VALUE = 0.0;
    private final double fillValue;

    /**
     * Constructor for TransformNullPlanNode.
     *
     * @param id node id
     * @param fillValue the value to replace nulls with
     */
    public TransformNullPlanNode(int id, double fillValue) {
        super(id);
        this.fillValue = fillValue;
    }

    @Override
    public <T> T accept(M3PlanVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String getExplainName() {
        return String.format(Locale.ROOT, "TRANSFORM_NULL(%s)", fillValue);
    }

    /**
     * Returns the value used to replace nulls.
     *
     * @return the fill value
     */
    public double getFillValue() {
        return fillValue;
    }

    /**
     * Factory method to create a TransformNullPlanNode from a FunctionNode.
     * Expects the function node to represent a TRANSFORM_NULL function with an optional fill value argument.
     *
     * @param functionNode the function node representing the TRANSFORM_NULL function
     * @return a new TransformNullPlanNode instance
     * @throws IllegalArgumentException if the function node has more than one argument or if the argument is not a valid number
     */
    public static TransformNullPlanNode of(FunctionNode functionNode) {
        if (functionNode.getChildren().isEmpty()) {
            return new TransformNullPlanNode(M3PlannerContext.generateId(), DEFAULT_VALUE);
        }
        if (functionNode.getChildren().size() > 1) {
            throw new IllegalArgumentException("transformNull function should have at most one argument");
        }
        if (!(functionNode.getChildren().getFirst() instanceof ValueNode valueNode)) {
            throw new IllegalArgumentException("Expected a value as the argument for transformNull");
        }

        String value = valueNode.getValue();
        try {
            double fillValue = Double.parseDouble(value);
            return new TransformNullPlanNode(M3PlannerContext.generateId(), fillValue);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid value for transformNull: " + value, e);
        }
    }
}
