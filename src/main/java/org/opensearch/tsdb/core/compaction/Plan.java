/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.compaction;

import org.opensearch.tsdb.core.index.closed.ClosedChunkIndex;

import java.util.List;

/**
 * Represents a compaction plan produced by a specific compaction strategy.
 * <p>
 * The plan carries the list of indexes to compact and a reference to the
 * {@link Compaction} that created it. When {@link Compaction#compact(Plan, ClosedChunkIndex)}
 * is invoked, the implementation can reject the call if the current strategy is no longer
 * the planner (e.g. after a dynamic settings change), so the client is forced to obtain
 * a new plan. This supports use cases where the client inspects the plan before deciding
 * to compact, without coupling plan and compact in a single run.
 */
public final class Plan {

    private final List<ClosedChunkIndex> indexes;
    private final Compaction planner;

    public Plan(List<ClosedChunkIndex> indexes, Compaction planner) {
        this.indexes = indexes == null ? List.of() : List.copyOf(indexes);
        this.planner = planner;
    }

    /** Indexes to compact, in the order determined by the planner. */
    public List<ClosedChunkIndex> getIndexes() {
        return indexes;
    }

    /** The compaction strategy that created this plan. */
    public Compaction getPlanner() {
        return planner;
    }

    public boolean isEmpty() {
        return indexes.isEmpty();
    }

    /** Whether the planner performs in-place compaction (no new index created). */
    public boolean isInPlaceCompaction() {
        return planner.isInPlaceCompaction();
    }
}
