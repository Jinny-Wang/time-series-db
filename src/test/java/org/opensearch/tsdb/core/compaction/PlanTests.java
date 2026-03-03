/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.compaction;

import org.junit.Assert;
import org.mockito.Mockito;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.core.index.closed.ClosedChunkIndex;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class PlanTests extends OpenSearchTestCase {

    public void testGetIndexesReturnsPlannedIndexes() {
        ClosedChunkIndex index1 = Mockito.mock(ClosedChunkIndex.class);
        ClosedChunkIndex index2 = Mockito.mock(ClosedChunkIndex.class);
        Compaction planner = new NoopCompaction();
        Plan plan = new Plan(List.of(index1, index2), planner);

        List<ClosedChunkIndex> indexes = plan.getIndexes();
        assertEquals(2, indexes.size());
        assertSame(index1, indexes.get(0));
        assertSame(index2, indexes.get(1));
    }

    public void testGetIndexesDefensiveCopy() {
        ClosedChunkIndex index1 = Mockito.mock(ClosedChunkIndex.class);
        List<ClosedChunkIndex> input = new java.util.ArrayList<>(List.of(index1));
        Plan plan = new Plan(input, new NoopCompaction());

        List<ClosedChunkIndex> got = plan.getIndexes();
        assertEquals(1, got.size());
        assertSame(index1, got.get(0));
        assertNotSame("getIndexes() should return a copy, not the original list", input, got);
    }

    public void testGetIndexesIsUnmodifiable() {
        Plan plan = new Plan(List.of(Mockito.mock(ClosedChunkIndex.class)), new NoopCompaction());
        List<ClosedChunkIndex> indexes = plan.getIndexes();
        Assert.assertThrows(UnsupportedOperationException.class, () -> indexes.add(Mockito.mock(ClosedChunkIndex.class)));
        Assert.assertThrows(UnsupportedOperationException.class, () -> indexes.remove(0));
    }

    public void testGetPlanner() {
        Compaction planner = new NoopCompaction();
        Plan plan = new Plan(List.of(), planner);
        assertSame(planner, plan.getPlanner());
    }

    public void testIsEmptyTrueWhenNoIndexes() {
        Plan plan = new Plan(List.of(), new NoopCompaction());
        assertTrue(plan.isEmpty());
    }

    public void testIsEmptyFalseWhenHasIndexes() {
        Plan plan = new Plan(List.of(Mockito.mock(ClosedChunkIndex.class)), new NoopCompaction());
        assertFalse(plan.isEmpty());
    }

    public void testNullIndexesTreatedAsEmpty() {
        Plan plan = new Plan(null, new NoopCompaction());
        assertTrue(plan.isEmpty());
        assertEquals(0, plan.getIndexes().size());
    }

    public void testIsInPlaceCompactionDelegatesToPlanner() {
        Compaction noop = new NoopCompaction();
        Plan plan = new Plan(List.of(), noop);
        assertFalse("NoopCompaction is not in-place", plan.isInPlaceCompaction());

        Compaction forceMerge = new ForceMergeCompaction(1L, 2, 1, 20 * 60 * 1000L, 2 * 60 * 60 * 1000L, TimeUnit.MILLISECONDS);
        Plan inPlacePlan = new Plan(List.of(), forceMerge);
        assertTrue("ForceMergeCompaction is in-place", inPlacePlan.isInPlaceCompaction());
    }
}
