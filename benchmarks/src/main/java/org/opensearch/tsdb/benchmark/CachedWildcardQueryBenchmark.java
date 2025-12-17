/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.benchmark;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KeywordField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.cache.Cache;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.TextFieldMapper;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.tsdb.TSDBPlugin;
import org.opensearch.tsdb.query.search.CachedWildcardQueryBuilder;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Benchmark comparing CachedWildcardQueryBuilder vs standard WildcardQueryBuilder.
 *
 * This benchmark measures the performance difference between:
 * 1. Query construction time (building the automaton)
 * 2. Full search execution time (construction + search)
 *
 * Parameterized by:
 * - patternType: PREFIX (foo-*), STANDALONE (*), SUFFIX (*-foo), COMPLEX (server-*-prod-*-us-*-cluster-*)
 * - matchCount: Number of matching documents (10, 1000)
 *
 * Expected results:
 * - Cache miss: ~same speed as standard (slight overhead for cache put)
 * - Cache hit: Much faster (skips automaton construction entirely)
 * - Benefit increases with pattern complexity
 */
@Fork(value = 1)
@Warmup(iterations = 3, time = 3, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 3, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
public class CachedWildcardQueryBenchmark {

    private static final String FIELD_NAME = "labels";
    private static final int TOTAL_DOC_COUNT = 10000;

    /**
     * Pattern types:
     * - PREFIX: foo-* (matches foo-1, foo-2, etc.)
     * - COMPLEX: server-*-prod-*-us-*-cluster-* (multiple wildcards)
     *  because we search labels with name:foo* or name:*foo or name:*foo
     *  it is either prefix or complext
     */
    @Param({ "PREFIX", "COMPLEX" })
    public String patternType;

    /**
     * Number of documents that match the wildcard pattern.
     * Controls selectivity of the query.
     */
    @Param({ "100" })
    public int matchCount;

    // Test infrastructure for query construction benchmarks
    private QueryShardContext mockContext;
    private String pattern;

    // Test infrastructure for search execution benchmarks
    private Directory directory;
    private DirectoryReader reader;
    private IndexSearcher searcher;
    private Query standardQuery;
    private Query cachedQuery;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        // Setup for query construction benchmarks
        setupQueryConstruction();

        // Setup for search execution benchmarks
        setupSearchExecution();

        printBenchmarkInfo();
    }

    private void setupQueryConstruction() {
        // Create mock QueryShardContext
        mockContext = mock(QueryShardContext.class);
        when(mockContext.allowExpensiveQueries()).thenReturn(true);

        // Create cluster-level Settings with cache enabled (max_size=1000) for benchmarking
        Settings settings = Settings.builder().put(TSDBPlugin.TSDB_ENGINE_WILDCARD_QUERY_CACHE_MAX_SIZE.getKey(), 1000).build();

        IndexSettings indexSettings = new IndexSettings(
            IndexMetadata.builder("benchmark-index")
                .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT).build())
                .numberOfShards(1)
                .numberOfReplicas(0)
                .build(),
            Settings.EMPTY
        );
        when(mockContext.getIndexSettings()).thenReturn(indexSettings);

        // Initialize cache using production method with ClusterSettings that includes plugin settings
        Set<Setting<?>> settingsSet = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        settingsSet.add(TSDBPlugin.TSDB_ENGINE_WILDCARD_QUERY_CACHE_MAX_SIZE);
        settingsSet.add(TSDBPlugin.TSDB_ENGINE_WILDCARD_QUERY_CACHE_EXPIRE_AFTER);
        ClusterSettings clusterSettings = new ClusterSettings(settings, settingsSet);
        CachedWildcardQueryBuilder.initializeCache(clusterSettings, settings);

        // Create mock field mapper
        MappedFieldType fieldType = new TextFieldMapper.TextFieldType(FIELD_NAME);
        when(mockContext.fieldMapper(FIELD_NAME)).thenReturn(fieldType);

        // Generate pattern based on type
        pattern = generatePattern();
    }

    private void setupSearchExecution() throws IOException {
        // Create in-memory directory
        directory = new ByteBuffersDirectory();

        // Create index with documents
        IndexWriterConfig config = new IndexWriterConfig();
        try (IndexWriter writer = new IndexWriter(directory, config)) {

            // Index documents that match the pattern
            for (int i = 0; i < matchCount; i++) {
                Document doc = new Document();
                String labelValue = generateMatchingLabel(i);
                doc.add(new KeywordField(FIELD_NAME, labelValue, Field.Store.NO));
                writer.addDocument(doc);
            }

            // Index documents that DON'T match the pattern (to reach TOTAL_DOC_COUNT)
            for (int i = matchCount; i < TOTAL_DOC_COUNT; i++) {
                Document doc = new Document();
                String labelValue = generateNonMatchingLabel(i);
                doc.add(new KeywordField(FIELD_NAME, labelValue, Field.Store.NO));
                writer.addDocument(doc);
            }

            writer.commit();
        }

        // Open reader and searcher
        reader = DirectoryReader.open(directory);
        searcher = new IndexSearcher(reader);

        // Pre-build queries for search benchmarks
        try {
            standardQuery = QueryBuilders.wildcardQuery(FIELD_NAME, pattern).toQuery(mockContext);
            cachedQuery = new CachedWildcardQueryBuilder(FIELD_NAME, pattern).toQuery(mockContext);
        } catch (IOException e) {
            throw new RuntimeException("Failed to build queries", e);
        }
    }

    private String generatePattern() {
        return switch (patternType) {
            case "PREFIX" -> "foo-*";
            case "COMPLEX" -> "server-*-prod-*-us-*-cluster-*";
            default -> throw new IllegalArgumentException("Unknown pattern type: " + patternType);
        };
    }

    private String generateMatchingLabel(int index) {
        return switch (patternType) {
            case "PREFIX" -> "foo-" + index;
            case "COMPLEX" -> "server-app" + index + "-prod-v1-us-west-cluster-main";
            default -> throw new IllegalArgumentException("Unknown pattern type: " + patternType);
        };
    }

    private String generateNonMatchingLabel(int index) {
        return switch (patternType) {
            case "PREFIX" -> "bar-" + index; // doesn't start with "foo-"
            case "COMPLEX" -> "other-app" + index + "-dev-v2-eu-east-node-backup";
            default -> throw new IllegalArgumentException("Unknown pattern type: " + patternType);
        };
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        if (reader != null) {
            reader.close();
        }
        if (directory != null) {
            directory.close();
        }
        CachedWildcardQueryBuilder.clearCache();
    }

    private void printBenchmarkInfo() {
        System.out.println("\n" + "=".repeat(80));
        System.out.println("WILDCARD QUERY CACHE BENCHMARK CONFIGURATION");
        System.out.println("=".repeat(80));
        System.out.println("Pattern type: " + patternType);
        System.out.println("Pattern: " + pattern);
        System.out.println("Total documents: " + TOTAL_DOC_COUNT);
        System.out.println("Matching documents: " + matchCount);
        System.out.println("Selectivity: " + String.format("%.2f%%", (matchCount * 100.0 / TOTAL_DOC_COUNT)));
        System.out.println("Cache enabled: YES (max_size=1000)");

        Cache.CacheStats stats = CachedWildcardQueryBuilder.getCacheStats();
        System.out.println(
            "Cache stats - Hits: "
                + stats.getHits()
                + ", Misses: "
                + stats.getMisses()
                + ", Size: "
                + CachedWildcardQueryBuilder.getCacheSize()
        );
        System.out.println("=".repeat(80) + "\n");
    }

    // ========================================
    // QUERY CONSTRUCTION BENCHMARKS
    // These measure ONLY the time to build the Query object (automaton construction)
    // ========================================

    /**
     * Baseline: Standard WildcardQueryBuilder (no caching).
     * Rebuilds automaton on every call.
     */
    @Benchmark
    public void queryConstruction_Standard(Blackhole bh) throws IOException {
        Query query = QueryBuilders.wildcardQuery(FIELD_NAME, pattern).toQuery(mockContext);
        bh.consume(query);
    }

    /**
     * Cached wildcard query - cache hit scenario.
     * Returns cached Query object (skips automaton construction).
     */
    @Benchmark
    public void queryConstruction_CachedHit(Blackhole bh) throws IOException {
        Query query = new CachedWildcardQueryBuilder(FIELD_NAME, pattern).toQuery(mockContext);
        bh.consume(query);
    }

    /**
     * Cached wildcard query - cache miss scenario.
     * Clears cache before each invocation to measure first-time performance.
     */
    @Benchmark
    public void queryConstruction_CachedMiss(Blackhole bh) throws IOException {
        CachedWildcardQueryBuilder.clearCache();
        Query query = new CachedWildcardQueryBuilder(FIELD_NAME, pattern).toQuery(mockContext);
        bh.consume(query);
    }

    // ========================================
    // FULL SEARCH EXECUTION BENCHMARKS
    // These measure query construction + actual search execution
    // ========================================

    /**
     * Full search with standard wildcard query.
     * Includes: Query construction (automaton) + search execution.
     */
    @Benchmark
    public void fullSearch_Standard(Blackhole bh) throws IOException {
        // Build query (includes automaton construction)
        Query query = QueryBuilders.wildcardQuery(FIELD_NAME, pattern).toQuery(mockContext);

        // Execute search
        TotalHits hits = searcher.search(query, matchCount + 100).totalHits;
        bh.consume(hits);
    }

    /**
     * Full search with cached wildcard query (cache hit).
     * Includes: Query construction (cached) + search execution.
     */
    @Benchmark
    public void fullSearch_CachedHit(Blackhole bh) throws IOException {
        // Build query (cache hit - no automaton construction)
        Query query = new CachedWildcardQueryBuilder(FIELD_NAME, pattern).toQuery(mockContext);

        // Execute search
        TotalHits hits = searcher.search(query, matchCount + 100).totalHits;
        bh.consume(hits);
    }

    /**
     * Full search with cached wildcard query (cache miss).
     * Includes: Query construction (cache miss + put) + search execution.
     */
    @Benchmark
    public void fullSearch_CachedMiss(Blackhole bh) throws IOException {
        CachedWildcardQueryBuilder.clearCache();

        // Build query (cache miss - builds automaton + caches it)
        Query query = new CachedWildcardQueryBuilder(FIELD_NAME, pattern).toQuery(mockContext);

        // Execute search
        TotalHits hits = searcher.search(query, matchCount + 100).totalHits;
        bh.consume(hits);
    }

    /**
     * Search execution ONLY (using pre-built query).
     * This isolates the search execution time without query construction overhead.
     * Useful for understanding what % of total time is query construction vs execution.
     */
    @Benchmark
    public void searchExecutionOnly_Standard(Blackhole bh) throws IOException {
        TotalHits hits = searcher.search(standardQuery, matchCount + 100).totalHits;
        bh.consume(hits);
    }

    /**
     * Search execution ONLY with cached query (using pre-built cached query).
     */
    @Benchmark
    public void searchExecutionOnly_Cached(Blackhole bh) throws IOException {
        TotalHits hits = searcher.search(cachedQuery, matchCount + 100).totalHits;
        bh.consume(hits);
    }
}
