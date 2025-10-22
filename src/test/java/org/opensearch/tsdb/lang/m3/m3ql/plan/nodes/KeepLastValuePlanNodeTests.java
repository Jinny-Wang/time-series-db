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
import org.opensearch.tsdb.lang.m3.m3ql.plan.visitor.M3PlanVisitor;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

/**
 * Unit tests for KeepLastValuePlanNode.
 */
public class KeepLastValuePlanNodeTests extends BasePlanNodeTests {

    public void testKeepLastValuePlanNodeCreation() {
        KeepLastValuePlanNode node = new KeepLastValuePlanNode(1, "1h");

        assertEquals(1, node.getId());
        assertEquals(Duration.ofHours(1), node.duration());
        assertEquals("KEEP_LAST_VALUE(1h)", node.getExplainName());
        assertTrue(node.getChildren().isEmpty());
    }

    public void testKeepLastValuePlanNodeWithEmptyLookback() {
        KeepLastValuePlanNode node = new KeepLastValuePlanNode(1, "");

        assertEquals(ChronoUnit.FOREVER.getDuration(), node.duration());
        assertEquals("KEEP_LAST_VALUE()", node.getExplainName());
    }

    public void testKeepLastValuePlanNodeVisitorAccept() {
        KeepLastValuePlanNode node = new KeepLastValuePlanNode(1, "5m");
        TestMockVisitor visitor = new TestMockVisitor();

        String result = node.accept(visitor);
        assertEquals("visit KeepLastValuePlanNode", result);
    }

    public void testKeepLastValuePlanNodeFactoryMethodWithArgument() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("keepLastValue");
        functionNode.addChildNode(new ValueNode("2h"));

        KeepLastValuePlanNode node = KeepLastValuePlanNode.of(functionNode);

        assertEquals(Duration.ofHours(2), node.duration());
    }

    public void testKeepLastValuePlanNodeFactoryMethodWithoutArgument() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("keepLastValue");

        KeepLastValuePlanNode node = KeepLastValuePlanNode.of(functionNode);

        assertEquals(ChronoUnit.FOREVER.getDuration(), node.duration());
    }

    public void testKeepLastValuePlanNodeFactoryMethodWithMinutes() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("keepLastValue");
        functionNode.addChildNode(new ValueNode("30m"));

        KeepLastValuePlanNode node = KeepLastValuePlanNode.of(functionNode);

        assertEquals(Duration.ofMinutes(30), node.duration());
    }

    public void testKeepLastValuePlanNodeFactoryMethodWithDays() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("keepLastValue");
        functionNode.addChildNode(new ValueNode("7d"));

        KeepLastValuePlanNode node = KeepLastValuePlanNode.of(functionNode);

        assertEquals(Duration.ofDays(7), node.duration());
    }

    public void testKeepLastValuePlanNodeFactoryMethodThrowsOnTooManyArguments() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("keepLastValue");
        functionNode.addChildNode(new ValueNode("1h"));
        functionNode.addChildNode(new ValueNode("2h"));

        expectThrows(IllegalArgumentException.class, () -> KeepLastValuePlanNode.of(functionNode));
    }

    public void testKeepLastValuePlanNodeFactoryMethodThrowsOnNonValueNode() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("keepLastValue");
        functionNode.addChildNode(new FunctionNode()); // throw on non value node

        expectThrows(IllegalArgumentException.class, () -> KeepLastValuePlanNode.of(functionNode));
    }

    public void testKeepLastValuePlanNodeThrowsOnNegativeLookback() {
        KeepLastValuePlanNode keepLastValuePlanNode = new KeepLastValuePlanNode(1, "-5m");

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, keepLastValuePlanNode::duration);
        assertEquals("Lookback duration cannot be negative: -5m", exception.getMessage());
    }

    private static class TestMockVisitor extends M3PlanVisitor<String> {
        @Override
        public String process(M3PlanNode planNode) {
            return "process called";
        }

        @Override
        public String visit(KeepLastValuePlanNode planNode) {
            return "visit KeepLastValuePlanNode";
        }
    }
}
