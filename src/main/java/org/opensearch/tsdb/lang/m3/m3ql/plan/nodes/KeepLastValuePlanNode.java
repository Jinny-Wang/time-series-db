/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.plan.nodes;

import org.opensearch.tsdb.lang.m3.common.M3Duration;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.FunctionNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.ValueNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.M3PlannerContext;
import org.opensearch.tsdb.lang.m3.m3ql.plan.visitor.M3PlanVisitor;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Locale;

/**
 * KeepLastValuePlanNode represents a plan node that keeps the last value of a metric within a specified lookback period.
 */
public class KeepLastValuePlanNode extends M3PlanNode {

    private final String lookback; // 2h, 5m etc.

    /**
     * Constructor for KeepLastValuePlanNode.
     *
     * @param id       node id
     * @param lookback the lookback duration for keeping the last value (e.g., "5m" for 5 minutes)
     */
    public KeepLastValuePlanNode(int id, String lookback) {
        super(id);
        this.lookback = lookback;
    }

    @Override
    public <T> T accept(M3PlanVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String getExplainName() {
        return String.format(Locale.ROOT, "KEEP_LAST_VALUE(%s)", lookback);
    }

    /**
     * Returns the lookback duration for keeping the last value.
     * If no lookback is specified, it returns a duration representing "forever".
     *
     * @return Duration object representing the lookback period
     */
    public Duration duration() {
        if (lookback.isEmpty()) {
            return ChronoUnit.FOREVER.getDuration(); // no limit on look back for keepLastValue without an arg
        }

        Duration duration = M3Duration.valueOf(lookback);
        if (duration.isNegative()) {
            throw new IllegalArgumentException("Lookback duration cannot be negative: " + lookback);
        }
        return duration;
    }

    /**
     * Factory method to create a KeepLastValuePlanNode from a FunctionNode.
     * Expects the function node to have zero or one child that is a ValueNode representing the lookback duration.
     *
     * @param functionNode the function node to convert
     * @return KeepLastValuePlanNode instance
     */
    public static KeepLastValuePlanNode of(FunctionNode functionNode) {
        if (functionNode.getChildren().size() > 1) {
            throw new IllegalArgumentException("KeepLastValue function can only have one argument");
        }

        String duration = "";
        if (functionNode.getChildren().size() == 1) {
            if (!(functionNode.getChildren().getFirst() instanceof ValueNode valueNode)) {
                throw new IllegalArgumentException("KeepLastValue function argument must be a value");
            }
            duration = valueNode.getValue();
        }
        return new KeepLastValuePlanNode(M3PlannerContext.generateId(), duration);
    }
}
