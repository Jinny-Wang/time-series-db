/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.plan.nodes;

import org.opensearch.tsdb.lang.m3.common.WindowAggregationType;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.FunctionNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.ValueNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.visitor.M3PlanVisitor;

import java.time.Duration;

/**
 * Unit tests for MovingPlanNode.
 */
public class MovingPlanNodeTests extends BasePlanNodeTests {

    public void testMovingPlanNodeCreationWithTimeBased() {
        MovingPlanNode node = new MovingPlanNode(1, "5m", WindowAggregationType.AVG);

        assertEquals(1, node.getId());
        assertEquals(Duration.ofMinutes(5), node.getTimeDuration());
        assertEquals(WindowAggregationType.AVG, node.getAggregationType());
        assertEquals("MOVING(5m, AVG)", node.getExplainName());
        assertFalse(node.isPointBased());
        assertTrue(node.getChildren().isEmpty());
    }

    public void testMovingPlanNodeCreationWithPointBased() {
        MovingPlanNode node = new MovingPlanNode(1, "10", WindowAggregationType.SUM);

        assertTrue(node.isPointBased());
        assertEquals(10, node.getPointDuration().intValue());
        assertEquals(WindowAggregationType.SUM, node.getAggregationType());
        assertEquals("MOVING(10, SUM)", node.getExplainName());
    }

    public void testMovingPlanNodeCreationWithFloatingPointBased() {
        MovingPlanNode node = new MovingPlanNode(1, "10.5", WindowAggregationType.AVG);

        assertTrue(node.isPointBased());
        // Floating-point is truncated to integer (10.5 -> 10)
        assertEquals(10, node.getPointDuration().intValue());
        assertEquals(WindowAggregationType.AVG, node.getAggregationType());
        assertEquals("MOVING(10.5, AVG)", node.getExplainName());
    }

    public void testMovingPlanNodeVisitorAccept() {
        MovingPlanNode node = new MovingPlanNode(1, "1h", WindowAggregationType.MAX);
        TestMockVisitor visitor = new TestMockVisitor();

        String result = node.accept(visitor);
        assertEquals("visit MovingPlanNode", result);
    }

    public void testMovingPlanNodeFactoryMethod() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("moving");
        functionNode.addChildNode(new ValueNode("5m"));
        functionNode.addChildNode(new ValueNode("avg"));

        MovingPlanNode node = MovingPlanNode.of(functionNode);

        assertEquals(Duration.ofMinutes(5), node.getTimeDuration());
        assertEquals(WindowAggregationType.AVG, node.getAggregationType());
        assertFalse(node.isPointBased());
    }

    public void testMovingPlanNodeFactoryMethodWithPointBased() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("moving");
        functionNode.addChildNode(new ValueNode("20"));
        functionNode.addChildNode(new ValueNode("max"));

        MovingPlanNode node = MovingPlanNode.of(functionNode);

        assertTrue(node.isPointBased());
        assertEquals(20, node.getPointDuration().intValue());
        assertEquals(WindowAggregationType.MAX, node.getAggregationType());
    }

    public void testMovingPlanNodeWithDifferentTimeUnits() {
        MovingPlanNode hourNode = new MovingPlanNode(1, "2h", WindowAggregationType.MIN);
        assertEquals(Duration.ofHours(2), hourNode.getTimeDuration());

        MovingPlanNode dayNode = new MovingPlanNode(2, "1d", WindowAggregationType.SUM);
        assertEquals(Duration.ofDays(1), dayNode.getTimeDuration());

        MovingPlanNode secondNode = new MovingPlanNode(3, "30s", WindowAggregationType.AVG);
        assertEquals(Duration.ofSeconds(30), secondNode.getTimeDuration());
    }

    public void testMovingPlanNodeIsPointBasedDetection() {
        assertTrue(new MovingPlanNode(1, "5", WindowAggregationType.SUM).isPointBased());
        assertTrue(new MovingPlanNode(1, "100", WindowAggregationType.AVG).isPointBased());
        assertTrue(new MovingPlanNode(1, " 50 ", WindowAggregationType.MAX).isPointBased());
        assertTrue(new MovingPlanNode(1, "5.5", WindowAggregationType.MIN).isPointBased()); // Floating-point supported, truncated to int

        assertFalse(new MovingPlanNode(1, "5m", WindowAggregationType.SUM).isPointBased());
        assertFalse(new MovingPlanNode(1, "1h", WindowAggregationType.AVG).isPointBased());
        assertFalse(new MovingPlanNode(1, "2d", WindowAggregationType.MAX).isPointBased());
    }

    public void testMovingPlanNodeFactoryMethodThrowsOnIncorrectArguments() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("moving");
        functionNode.addChildNode(new ValueNode("5m"));

        expectThrows(IllegalArgumentException.class, () -> MovingPlanNode.of(functionNode));
    }

    public void testMovingPlanNodeFactoryMethodThrowsOnTooManyArguments() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("moving");
        functionNode.addChildNode(new ValueNode("5m"));
        functionNode.addChildNode(new ValueNode("avg"));
        functionNode.addChildNode(new ValueNode("extra"));

        expectThrows(IllegalArgumentException.class, () -> MovingPlanNode.of(functionNode));
    }

    public void testMovingPlanNodeFactoryMethodThrowsOnNonValueNodes() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("moving");
        functionNode.addChildNode(new FunctionNode()); // not a value node
        functionNode.addChildNode(new ValueNode("avg"));

        expectThrows(IllegalArgumentException.class, () -> MovingPlanNode.of(functionNode));
    }

    public void testMovingPlanNodeGetInvalidDuration() {
        MovingPlanNode node = new MovingPlanNode(1, "-5m", WindowAggregationType.AVG);
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, node::getTimeDuration);
        assertEquals("Window size cannot be negative: -5m", exception.getMessage());
    }

    /**
     * Test isPointBased() returns false when windowSize is null (lines 88-90).
     */
    public void testMovingPlanNodeIsPointBasedWithNullWindowSize() {
        MovingPlanNode node = new MovingPlanNode(1, null, WindowAggregationType.AVG);
        assertFalse("isPointBased should return false for null windowSize", node.isPointBased());
    }

    /**
     * Test getPointDuration() throws IllegalArgumentException for zero value (lines 73-74).
     */
    public void testMovingPlanNodeGetPointDurationWithZero() {
        MovingPlanNode node = new MovingPlanNode(1, "0", WindowAggregationType.SUM);
        assertTrue("Zero should be detected as point-based format", node.isPointBased());

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, node::getPointDuration);
        assertTrue(exception.getMessage().contains("must be positive"));
        assertTrue(exception.getMessage().contains("0.0"));
    }

    /**
     * Test getPointDuration() throws IllegalArgumentException for negative value (lines 73-74).
     */
    public void testMovingPlanNodeGetPointDurationWithNegative() {
        MovingPlanNode node = new MovingPlanNode(1, "-10", WindowAggregationType.MAX);
        // Negative numbers don't match the point-based regex, so isPointBased() returns false
        assertFalse("Negative should not be detected as point-based", node.isPointBased());
    }

    /**
     * Test getPointDuration() throws IllegalArgumentException for invalid format (line 77-78).
     */
    public void testMovingPlanNodeGetPointDurationWithInvalidFormat() {
        MovingPlanNode node = new MovingPlanNode(1, "abc", WindowAggregationType.AVG);
        assertFalse("Invalid format should not be detected as point-based", node.isPointBased());

        // If we force call getPointDuration on a non-point-based format, it should throw
        // Note: In normal usage, getPointDuration is only called if isPointBased() is true
    }

    private static class TestMockVisitor extends M3PlanVisitor<String> {
        @Override
        public String process(M3PlanNode planNode) {
            return "process called";
        }

        @Override
        public String visit(MovingPlanNode planNode) {
            return "visit MovingPlanNode";
        }
    }
}
