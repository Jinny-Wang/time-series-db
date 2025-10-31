/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.plan.nodes;

import org.opensearch.tsdb.lang.m3.common.Utils;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.FunctionNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.M3ASTNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.ValueNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.M3PlannerContext;
import org.opensearch.tsdb.lang.m3.m3ql.plan.visitor.M3PlanVisitor;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * AliasByTagsPlanNode represents a node in the M3QL plan that renames series based on available tag values.
 * Takes multiple tag names and will rename the series based on available tags. If a tag value is not found, it is ignored.
 */
public class AliasByTagsPlanNode extends M3PlanNode {
    private final List<String> tagNames;

    /**
     * Constructor for AliasByTagsPlanNode.
     *
     * @param id node id
     * @param tagNames the list of tag names to use for building the alias
     */
    public AliasByTagsPlanNode(int id, List<String> tagNames) {
        super(id);
        this.tagNames = new ArrayList<>(tagNames);
    }

    @Override
    public <T> T accept(M3PlanVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String getExplainName() {
        return String.format(Locale.ROOT, "ALIAS_BY_TAGS(%s)", String.join(", ", tagNames));
    }

    /**
     * Returns the list of tag names used for aliasing.
     * @return List of tag names
     */
    public List<String> getTagNames() {
        return new ArrayList<>(tagNames);
    }

    /**
     * Factory method to create an AliasByTagsPlanNode from a FunctionNode.
     * Expects the function node to have children that are ValueNodes representing tag names.
     *
     * @param functionNode the function node to convert
     * @return an instance of AliasByTagsPlanNode
     */
    public static AliasByTagsPlanNode of(FunctionNode functionNode) {
        List<M3ASTNode> childNodes = functionNode.getChildren();
        List<String> tagNames = new ArrayList<>();
        for (M3ASTNode child : childNodes) {
            if (!(child instanceof ValueNode valueNode)) {
                throw new IllegalStateException("AliasByTags function expects values as arguments");
            }

            String tagName = Utils.stripDoubleQuotes(valueNode.getValue());
            tagNames.add(tagName);
        }

        return new AliasByTagsPlanNode(M3PlannerContext.generateId(), tagNames);
    }
}
