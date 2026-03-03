/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.compaction;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.index.IndexSettings;
import org.opensearch.tsdb.TSDBPlugin;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.TimeUnit;

import org.opensearch.tsdb.core.index.closed.ClosedChunkIndex;

/**
 * Factory class for creating compaction strategy instances based on index settings.
 * <p>
 * This factory determines the appropriate compaction strategy to use for an index
 * based on its configuration. Currently supported strategies include:
 * <ul>
 *   <li>SizeTieredCompaction - Size-tiered compaction with configurable time ranges</li>
 *   <li>ForceMergeCompaction - In-place force merge optimization for multi-segment indexes</li>
 *   <li>NoopCompaction - Default strategy that performs no compaction</li>
 * </ul>
 */
public class CompactionFactory {
    private static final Logger logger = LogManager.getLogger(CompactionFactory.class);

    public enum CompactionType {
        SizeTieredCompaction("SizeTieredCompaction"),
        ForceMergeCompaction("ForceMergeCompaction"),
        Noop("Noop"),
        Invalid("Invalid");

        public final String name;

        CompactionType(String name) {
            this.name = name;
        }

        public static CompactionType from(String compactionType) {
            return switch (compactionType) {
                case "SizeTieredCompaction" -> CompactionType.SizeTieredCompaction;
                case "ForceMergeCompaction" -> CompactionType.ForceMergeCompaction;
                case "Noop" -> CompactionType.Noop;
                default -> CompactionType.Invalid;
            };
        }
    }

    /**
     * Creates a compaction strategy instance based on the provided index settings.
     * Returns a delegating compaction that updates when dynamic settings change
     * (compaction type, force-merge settings, or frequency).
     *
     * @param indexSettings the index settings containing compaction and retention configuration
     * @return a Compaction instance configured according to the index settings
     */
    public static Compaction create(IndexSettings indexSettings) {
        Compaction initial = getCompactionFor(indexSettings);
        DelegatingCompaction delegating = new DelegatingCompaction(initial);

        // IndexScopedSettings only provides 1- and 2-setting consumers (no 4-arg overload), so we use two
        // grouped consumers so all updates use callback values instead of stale indexSettings.
        indexSettings.getScopedSettings()
            .addSettingsUpdateConsumer(
                TSDBPlugin.TSDB_ENGINE_COMPACTION_TYPE,
                TSDBPlugin.TSDB_ENGINE_COMPACTION_FREQUENCY,
                (newType, newFrequency) -> {
                    logger.info("Updating compaction type to: {}, frequency to: {}", newType, newFrequency);
                    delegating.setCompaction(getCompactionFor(indexSettings, newType, newFrequency.getMillis(), null, null));
                }
            );
        indexSettings.getScopedSettings()
            .addSettingsUpdateConsumer(
                TSDBPlugin.TSDB_ENGINE_FORCE_MERGE_MIN_SEGMENT_COUNT,
                TSDBPlugin.TSDB_ENGINE_FORCE_MERGE_MAX_SEGMENTS_AFTER_MERGE,
                (newMinSegments, newMaxSegments) -> {
                    logger.info(
                        "Updating force merge settings: min_segment_count={}, max_segments_after_merge={}",
                        newMinSegments,
                        newMaxSegments
                    );
                    delegating.setCompaction(getCompactionFor(indexSettings, null, null, newMinSegments, newMaxSegments));
                }
            );

        return delegating;
    }

    /**
     * Wrapper that delegates to the current compaction and allows swapping the delegate
     * when dynamic settings (type or force-merge config) change.
     * Package-private for tests that need to assert on the underlying compaction type.
     * <p>
     * Uses {@link Plan} to carry planner identity. {@link #compact(Plan, ClosedChunkIndex)} rejects
     * the call if the plan was created by a different strategy (e.g. after a settings change),
     * so the client must obtain a new plan.
     */
    static class DelegatingCompaction implements Compaction {
        private final AtomicReference<Compaction> current;

        DelegatingCompaction(Compaction initial) {
            this.current = new AtomicReference<>(initial);
        }

        void setCompaction(Compaction compaction) {
            this.current.set(compaction);
        }

        /** Returns the current compaction delegate. Used by tests to assert on concrete type. */
        Compaction getCurrent() {
            return current.get();
        }

        @Override
        public Plan plan(List<ClosedChunkIndex> indexes) {
            return current.get().plan(indexes);
        }

        @Override
        public void compact(Plan plan, ClosedChunkIndex dest) throws IOException {
            Compaction planner = plan.getPlanner();
            if (planner != current.get()) {
                throw new IllegalStateException(
                    "Compaction strategy changed since plan was created; obtain a new plan with plan() before compacting"
                );
            }
            planner.compact(plan, dest);
        }

        @Override
        public boolean isInPlaceCompaction() {
            return current.get().isInPlaceCompaction();
        }

        @Override
        public long getFrequency() {
            return current.get().getFrequency();
        }

        @Override
        public void setFrequency(long frequency) {
            current.get().setFrequency(frequency);
        }
    }

    private static Compaction getCompactionFor(IndexSettings indexSettings) {
        return getCompactionFor(indexSettings, null, null, null, null);
    }

    private static Compaction getCompactionFor(IndexSettings indexSettings, String compactionTypeOverride) {
        return getCompactionFor(indexSettings, compactionTypeOverride, null, null, null);
    }

    /**
     * Creates a compaction for the given index settings, with optional overrides for values that
     * may not yet be reflected in indexSettings when update consumers run (consumers are invoked
     * before IndexSettings is updated). Use overrides from the consumer callback arguments so
     * runtime updates are applied correctly.
     *
     * @param frequencyMillisOverride            when non-null, use for all compaction types instead of settings
     * @param forceMergeMinSegmentCountOverride  when non-null, use for ForceMergeCompaction instead of settings
     * @param forceMergeMaxSegmentsOverride      when non-null, use for ForceMergeCompaction instead of settings
     */
    private static Compaction getCompactionFor(
        IndexSettings indexSettings,
        String compactionTypeOverride,
        Long frequencyMillisOverride,
        Integer forceMergeMinSegmentCountOverride,
        Integer forceMergeMaxSegmentsOverride
    ) {
        CompactionType compactionType = compactionTypeOverride != null
            ? CompactionType.from(compactionTypeOverride)
            : CompactionType.from(TSDBPlugin.TSDB_ENGINE_COMPACTION_TYPE.get(indexSettings.getSettings()));

        // Read common settings used by multiple compaction types (use override when provided by callback)
        long frequency = frequencyMillisOverride != null
            ? frequencyMillisOverride
            : TSDBPlugin.TSDB_ENGINE_COMPACTION_FREQUENCY.get(indexSettings.getSettings()).getMillis();
        TimeUnit resolution = TimeUnit.valueOf(TSDBPlugin.TSDB_ENGINE_TIME_UNIT.get(indexSettings.getSettings()));

        switch (compactionType) {
            case SizeTieredCompaction:
                long retentionTime = TSDBPlugin.TSDB_ENGINE_RETENTION_TIME.get(indexSettings.getSettings()).getHours();
                long ttl = retentionTime != -1 ? retentionTime : Long.MAX_VALUE;

                // Cap the max index size as minimum of 1/10 of TTL or 31D(744H).
                List<Integer> tiers = new ArrayList<>();
                for (int tier = 2;; tier *= 3) {
                    if (tier > ttl * 0.1) {
                        if (tier > 744) {
                            tiers.add(744);
                        }
                        break;
                    }
                    tiers.add(tier);
                }

                return new SizeTieredCompaction(tiers.stream().map(Duration::ofHours).toArray(Duration[]::new), frequency, resolution);
            case ForceMergeCompaction:
                int minSegmentCount = forceMergeMinSegmentCountOverride != null
                    ? forceMergeMinSegmentCountOverride
                    : TSDBPlugin.TSDB_ENGINE_FORCE_MERGE_MIN_SEGMENT_COUNT.get(indexSettings.getSettings());
                int maxSegmentsAfterForceMerge = forceMergeMaxSegmentsOverride != null
                    ? forceMergeMaxSegmentsOverride
                    : TSDBPlugin.TSDB_ENGINE_FORCE_MERGE_MAX_SEGMENTS_AFTER_MERGE.get(indexSettings.getSettings());
                long oooCutoffWindow = TSDBPlugin.TSDB_ENGINE_OOO_CUTOFF.get(indexSettings.getSettings()).getMillis();
                long blockDuration = TSDBPlugin.TSDB_ENGINE_BLOCK_DURATION.get(indexSettings.getSettings()).getMillis();
                return new ForceMergeCompaction(
                    frequency,
                    minSegmentCount,
                    maxSegmentsAfterForceMerge,
                    oooCutoffWindow,
                    blockDuration,
                    resolution
                );
            case Noop:
                return new NoopCompaction();
            default:
                throw new IllegalArgumentException("Unknown compaction type: " + compactionType);
        }
    }
}
