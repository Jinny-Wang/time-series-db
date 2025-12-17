/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.search;

import org.apache.lucene.search.Query;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.cache.Cache;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.TextFieldMapper;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.TSDBPlugin;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CachedWildcardQueryBuilderTests extends OpenSearchTestCase {

    private MappedFieldType fieldType1 = null;
    private MappedFieldType fieldType2 = null;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        // Clear cache before each test to ensure test isolation
        CachedWildcardQueryBuilder.clearCache();
        // Reset field type references
        fieldType1 = null;
        fieldType2 = null;
    }

    /**
     * Helper method to create a mock QueryShardContext with cache enabled
     */
    private QueryShardContext createMockContext() {
        return createMockContextWithCacheSize(1000);
    }

    /**
     * Helper method to create a mock QueryShardContext with default settings (cache disabled)
     */
    private QueryShardContext createMockContextWithDefaultSettings() {
        return createMockContextWithCacheSize(0);
    }

    /**
     * Helper method to create a mock QueryShardContext with specified cache size
     */
    private QueryShardContext createMockContextWithCacheSize(int cacheSize) {
        return createMockContextWithCacheSettings(cacheSize, TimeValue.timeValueHours(1));
    }

    /**
     * Helper method to create a mock QueryShardContext with specified cache size and expiration
     */
    private QueryShardContext createMockContextWithCacheSettings(int cacheSize, TimeValue expiration) {
        QueryShardContext context = mock(QueryShardContext.class);

        // Allow expensive queries (required for wildcard queries)
        when(context.allowExpensiveQueries()).thenReturn(true);

        // Create cluster-level Settings with specified cache size and expiration
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
            .put(TSDBPlugin.TSDB_ENGINE_WILDCARD_QUERY_CACHE_MAX_SIZE.getKey(), cacheSize)
            .put(TSDBPlugin.TSDB_ENGINE_WILDCARD_QUERY_CACHE_EXPIRE_AFTER.getKey(), expiration)
            .build();

        IndexSettings indexSettings = new IndexSettings(
            IndexMetadata.builder("test-index")
                .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT).build())
                .numberOfShards(1)
                .numberOfReplicas(0)
                .build(),
            Settings.EMPTY
        );
        when(context.getIndexSettings()).thenReturn(indexSettings);

        // Initialize cache using production method with ClusterSettings that includes plugin settings
        Set<Setting<?>> settingsSet = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        settingsSet.add(TSDBPlugin.TSDB_ENGINE_WILDCARD_QUERY_CACHE_MAX_SIZE);
        settingsSet.add(TSDBPlugin.TSDB_ENGINE_WILDCARD_QUERY_CACHE_EXPIRE_AFTER);
        ClusterSettings clusterSettings = new ClusterSettings(settings, settingsSet);
        CachedWildcardQueryBuilder.initializeCache(clusterSettings, settings);

        // Create spy field mappers so we can verify wildcardQuery() calls
        // Using spy allows us to track method calls while keeping real behavior
        fieldType1 = spy(new TextFieldMapper.TextFieldType("field1"));
        when(context.fieldMapper("field1")).thenReturn(fieldType1);

        fieldType2 = spy(new TextFieldMapper.TextFieldType("field2"));
        when(context.fieldMapper("field2")).thenReturn(fieldType2);

        return context;
    }

    // ========================================
    // BASIC FUNCTIONALITY TESTS
    // ========================================

    /**
     * Test: Verify constructor sets field name and value correctly
     */
    public void testConstructor() {
        CachedWildcardQueryBuilder builder = new CachedWildcardQueryBuilder("testField", "testPattern*");

        assertThat("Field name should be set correctly", builder.fieldName(), equalTo("testField"));
        assertThat("Value should be set correctly", builder.value(), equalTo("testPattern*"));
        assertThat("Case insensitive should default to false", builder.caseInsensitive(), equalTo(false));
        assertThat("Rewrite should default to null", builder.rewrite(), equalTo(null));
    }

    /**
     * Test: Verify builder methods work correctly
     */
    public void testBuilderMethods() {
        CachedWildcardQueryBuilder builder = new CachedWildcardQueryBuilder("field", "pattern*");
        builder.caseInsensitive(true);
        builder.rewrite("constant_score");
        builder.boost(2.0f);
        builder.queryName("myQuery");

        assertThat(builder.fieldName(), equalTo("field"));
        assertThat(builder.value(), equalTo("pattern*"));
        assertThat(builder.caseInsensitive(), equalTo(true));
        assertThat(builder.rewrite(), equalTo("constant_score"));
        assertThat(builder.boost(), equalTo(2.0f));
        assertThat(builder.queryName(), equalTo("myQuery"));
    }

    /**
     * Test: Verify getWriteableName and getName return correct values
     */
    public void testGetNames() {
        CachedWildcardQueryBuilder builder = new CachedWildcardQueryBuilder("field1", "pattern*");

        assertThat("Writeable name should be 'cached_wildcard'", builder.getWriteableName(), equalTo("cached_wildcard"));
        assertThat("Name should be 'cached_wildcard'", builder.getName(), equalTo("cached_wildcard"));
    }

    // ========================================
    // SERIALIZATION TESTS
    // ========================================

    /**
     * Helper to create random query builder for testing
     */
    private CachedWildcardQueryBuilder createRandomQueryBuilder() {
        String fieldName = randomFrom("field1", "field2", "labels", "name");
        String pattern = randomFrom("pattern*", "server-*", "db-*", "*test", "foo?bar", "region:us-*");

        CachedWildcardQueryBuilder builder = new CachedWildcardQueryBuilder(fieldName, pattern);

        if (randomBoolean()) {
            builder.caseInsensitive(randomBoolean());
        }
        if (randomBoolean()) {
            builder.rewrite(randomFrom("constant_score", "scoring_boolean", "constant_score_boolean"));
        }
        if (randomBoolean()) {
            builder.boost(randomFloat());
        }
        if (randomBoolean()) {
            builder.queryName(randomAlphaOfLength(10));
        }

        return builder;
    }

    /**
     * Test: Stream serialization round-trip with minimal parameters
     */
    public void testStreamSerializationMinimal() throws IOException {
        CachedWildcardQueryBuilder original = new CachedWildcardQueryBuilder("field1", "pattern*");

        // Serialize to stream
        BytesStreamOutput output = new BytesStreamOutput();
        original.writeTo(output);

        // Deserialize from stream
        StreamInput input = output.bytes().streamInput();
        CachedWildcardQueryBuilder deserialized = new CachedWildcardQueryBuilder(input);

        // Verify all fields match
        assertEquals(original.fieldName(), deserialized.fieldName());
        assertEquals(original.value(), deserialized.value());
        assertEquals(original.caseInsensitive(), deserialized.caseInsensitive());
        assertEquals(original.rewrite(), deserialized.rewrite());
        assertEquals(original.boost(), deserialized.boost(), 0.0f);
        assertEquals(original.queryName(), deserialized.queryName());
    }

    /**
     * Test: Stream serialization round-trip with all parameters
     */
    public void testStreamSerializationComplete() throws IOException {
        CachedWildcardQueryBuilder original = new CachedWildcardQueryBuilder("myField", "my*pattern?");
        original.caseInsensitive(true);
        original.rewrite("constant_score");
        original.boost(3.5f);
        original.queryName("testQuery");

        // Serialize to stream
        BytesStreamOutput output = new BytesStreamOutput();
        original.writeTo(output);

        // Deserialize from stream
        StreamInput input = output.bytes().streamInput();
        CachedWildcardQueryBuilder deserialized = new CachedWildcardQueryBuilder(input);

        // Verify all fields
        assertEquals(original.fieldName(), deserialized.fieldName());
        assertEquals(original.value(), deserialized.value());
        assertEquals(original.caseInsensitive(), deserialized.caseInsensitive());
        assertEquals(original.rewrite(), deserialized.rewrite());
        assertEquals(original.boost(), deserialized.boost(), 0.0f);
        assertEquals(original.queryName(), deserialized.queryName());
    }

    /**
     * Test: XContent serialization round-trip
     */
    public void testXContentRoundTrip() throws IOException {
        for (int i = 0; i < 10; i++) {
            CachedWildcardQueryBuilder original = createRandomQueryBuilder();

            XContentType xContentType = randomFrom(XContentType.values());
            BytesReference bytes = XContentHelper.toXContent(original, xContentType, false);

            try (XContentParser parser = createParser(xContentType.xContent(), bytes)) {
                // AbstractQueryTestCase doesn't need manual parser positioning because it uses
                // framework methods (parseQuery/assertParsedQuery) that handle it automatically.
                // Since we're calling fromXContent() directly, we need to position the parser manually.
                // XContent structure: {"cached_wildcard": {"field": {"value": "pattern*"}}}
                parser.nextToken(); // Move to START_OBJECT (outer {)
                parser.nextToken(); // Move to FIELD_NAME ("cached_wildcard")
                parser.nextToken(); // Move to START_OBJECT (the { after "cached_wildcard")

                CachedWildcardQueryBuilder deserialized = CachedWildcardQueryBuilder.fromXContent(parser);

                assertEquals(original.fieldName(), deserialized.fieldName());
                assertEquals(original.value(), deserialized.value());
                assertEquals(original.caseInsensitive(), deserialized.caseInsensitive());
                assertEquals(original.rewrite(), deserialized.rewrite());
                assertEquals(original.boost(), deserialized.boost(), 0.0f);
                assertEquals(original.queryName(), deserialized.queryName());
            }
        }
    }

    // ========================================
    // EQUALS AND HASHCODE TESTS
    // ========================================

    /**
     * Test: equals returns true for identical builders
     */
    public void testEquals() {
        CachedWildcardQueryBuilder builder1 = new CachedWildcardQueryBuilder("field", "pattern*");
        builder1.caseInsensitive(true);
        builder1.rewrite("constant_score");

        CachedWildcardQueryBuilder builder2 = new CachedWildcardQueryBuilder("field", "pattern*");
        builder2.caseInsensitive(true);
        builder2.rewrite("constant_score");

        assertEquals("Builders with same parameters should be equal", builder1, builder2);
        assertEquals("Equal builders should have same hashCode", builder1.hashCode(), builder2.hashCode());
    }

    /**
     * Test: equals returns false for different field names
     */
    public void testNotEqualsDifferentField() {
        CachedWildcardQueryBuilder builder1 = new CachedWildcardQueryBuilder("field1", "pattern*");
        CachedWildcardQueryBuilder builder2 = new CachedWildcardQueryBuilder("field2", "pattern*");

        assertNotEquals("Builders with different field names should not be equal", builder1, builder2);
    }

    /**
     * Test: equals returns false for different patterns
     */
    public void testNotEqualsDifferentPattern() {
        CachedWildcardQueryBuilder builder1 = new CachedWildcardQueryBuilder("field", "pattern1*");
        CachedWildcardQueryBuilder builder2 = new CachedWildcardQueryBuilder("field", "pattern2*");

        assertNotEquals("Builders with different patterns should not be equal", builder1, builder2);
    }

    /**
     * Test: equals returns false for different case sensitivity
     */
    public void testNotEqualsDifferentCaseSensitivity() {
        CachedWildcardQueryBuilder builder1 = new CachedWildcardQueryBuilder("field", "pattern*");
        builder1.caseInsensitive(false);

        CachedWildcardQueryBuilder builder2 = new CachedWildcardQueryBuilder("field", "pattern*");
        builder2.caseInsensitive(true);

        assertNotEquals("Builders with different case sensitivity should not be equal", builder1, builder2);
    }

    // ========================================
    // DEFAULT BEHAVIOR TESTS (CACHE DISABLED)
    // ========================================

    /**
     * Test 0: Verify that with default settings (cache disabled), CachedWildcardQueryBuilder
     * behaves the same as WildcardQueryBuilder - queries work but no caching occurs.
     */
    public void testDefaultSettingsNoCaching() throws IOException {
        // Use default settings (cache disabled)
        QueryShardContext context = createMockContextWithDefaultSettings();

        // Get initial cache stats
        long initialHits = CachedWildcardQueryBuilder.getCacheHits();
        long initialSize = CachedWildcardQueryBuilder.getCacheSize();

        // Execute same query multiple times
        CachedWildcardQueryBuilder builder1 = new CachedWildcardQueryBuilder("field1", "server-*");
        Query query1 = builder1.toQuery(context);
        assertNotNull("Query should be created even with cache disabled", query1);

        CachedWildcardQueryBuilder builder2 = new CachedWildcardQueryBuilder("field1", "server-*");
        Query query2 = builder2.toQuery(context);
        assertNotNull("Query should be created even with cache disabled", query2);

        CachedWildcardQueryBuilder builder3 = new CachedWildcardQueryBuilder("field1", "server-*");
        Query query3 = builder3.toQuery(context);
        assertNotNull("Query should be created even with cache disabled", query3);

        // Verify no actual caching occurred (most important checks)
        long finalHits = CachedWildcardQueryBuilder.getCacheHits();
        long finalSize = CachedWildcardQueryBuilder.getCacheSize();

        assertThat("No cache hits should occur with cache disabled", finalHits, equalTo(initialHits));
        assertThat("Cache size should remain 0 with cache disabled", finalSize, equalTo(initialSize));
        assertThat("Cache size should be 0", finalSize, equalTo(0L));

        // Verify queries are NOT the same instance (no caching)
        // This is the key behavior: even though the same pattern is used, queries are not reused
        assertThat("Queries should not be cached when cache is disabled", query2, not(sameInstance(query1)));
        assertThat("Queries should not be cached when cache is disabled", query3, not(sameInstance(query1)));
    }

    // ========================================
    // CACHE BEHAVIOR TESTS
    // ========================================

    /**
     * Test 1: Verify cache miss on first query execution
     */
    public void testCacheMissOnFirstQuery() throws IOException {
        // Get initial stats
        Cache.CacheStats initialStats = CachedWildcardQueryBuilder.getCacheStats();
        long initialMisses = initialStats.getMisses();

        // Create query builder
        CachedWildcardQueryBuilder builder = new CachedWildcardQueryBuilder("field1", "pattern*");

        // Execute query (this triggers doToQuery)
        QueryShardContext context = createMockContext();
        Query query1 = builder.toQuery(context);

        // Verify cache miss
        Cache.CacheStats afterStats = CachedWildcardQueryBuilder.getCacheStats();
        assertThat("Cache should have one miss", afterStats.getMisses(), equalTo(initialMisses + 1));
        assertNotNull("Query should be created", query1);

        // Verify that wildcardQuery was actually called (cache miss)
        verify(fieldType1, times(1)).wildcardQuery(eq("pattern*"), any(), anyBoolean(), any());
    }

    /**
     * Test 2: Verify cache hit on repeated query with same pattern
     */
    public void testCacheHitOnRepeatedQuery() throws IOException {
        // Create first query builder and execute
        CachedWildcardQueryBuilder builder1 = new CachedWildcardQueryBuilder("field1", "server-*");
        QueryShardContext context = createMockContext();
        Query query1 = builder1.toQuery(context);

        // Verify wildcardQuery was called for first query (cache miss)
        verify(fieldType1, times(1)).wildcardQuery(eq("server-*"), any(), anyBoolean(), any());

        // Get stats after first query
        Cache.CacheStats afterFirstQuery = CachedWildcardQueryBuilder.getCacheStats();
        long missesAfterFirst = afterFirstQuery.getMisses();
        long hitsAfterFirst = afterFirstQuery.getHits();

        // Create second query builder with SAME pattern and execute
        CachedWildcardQueryBuilder builder2 = new CachedWildcardQueryBuilder("field1", "server-*");
        Query query2 = builder2.toQuery(context);

        // Get stats after second query
        Cache.CacheStats afterSecondQuery = CachedWildcardQueryBuilder.getCacheStats();

        // Verify cache hit
        assertThat("Cache should have one more hit", afterSecondQuery.getHits(), equalTo(hitsAfterFirst + 1));
        assertThat("Cache misses should not increase", afterSecondQuery.getMisses(), equalTo(missesAfterFirst));

        // Verify same Query object is returned (object identity)
        assertThat("Should return same Query object from cache", query2, sameInstance(query1));

        // CRITICAL: Verify wildcardQuery was NOT called again (cache hit - no query construction!)
        verify(fieldType1, times(1)).wildcardQuery(eq("server-*"), any(), anyBoolean(), any());
    }

    /**
     * Test 3: Verify different patterns create different cache entries
     */
    public void testDifferentPatternsCacheSeparately() throws IOException {

        QueryShardContext context = createMockContext();

        // Execute query with pattern1
        CachedWildcardQueryBuilder builder1 = new CachedWildcardQueryBuilder("field1", "server-*");
        Query query1 = builder1.toQuery(context);

        // Execute query with pattern2 (different pattern)
        CachedWildcardQueryBuilder builder2 = new CachedWildcardQueryBuilder("field1", "db-*");
        Query query2 = builder2.toQuery(context);

        // Execute query with pattern1 again
        CachedWildcardQueryBuilder builder3 = new CachedWildcardQueryBuilder("field1", "server-*");
        Query query3 = builder3.toQuery(context);

        // Get final stats
        Cache.CacheStats finalStats = CachedWildcardQueryBuilder.getCacheStats();

        // Should have 2 misses (pattern1 and pattern2) and 1 hit (pattern1 repeated)
        assertThat("Should have 2 cache misses", finalStats.getMisses(), equalTo(2L));
        assertThat("Should have 1 cache hit", finalStats.getHits(), equalTo(1L));

        // Verify query1 and query3 are same object (cached)
        assertThat("Same pattern should return same Query object", query3, sameInstance(query1));

        // Verify query1 and query2 are different objects
        assertThat("Different patterns should create different Query objects", query1, not(sameInstance(query2)));

        // Verify wildcardQuery was called for each unique pattern
        verify(fieldType1, times(1)).wildcardQuery(eq("server-*"), any(), anyBoolean(), any());
        verify(fieldType1, times(1)).wildcardQuery(eq("db-*"), any(), anyBoolean(), any());
        // Total calls should be 2 (not 3, because third query was a cache hit)
        verify(fieldType1, times(2)).wildcardQuery(anyString(), any(), anyBoolean(), any());
    }

    /**
     * Test 4: Verify different field names create different cache entries
     */
    public void testDifferentFieldsCacheSeparately() throws IOException {

        QueryShardContext context = createMockContext();

        // Same pattern, different fields
        CachedWildcardQueryBuilder builder1 = new CachedWildcardQueryBuilder("field1", "server-*");
        Query query1 = builder1.toQuery(context);

        CachedWildcardQueryBuilder builder2 = new CachedWildcardQueryBuilder("field2", "server-*");
        Query query2 = builder2.toQuery(context);

        // Get stats
        Cache.CacheStats stats = CachedWildcardQueryBuilder.getCacheStats();

        // Both should be cache misses (different fields = different cache keys)
        assertThat("Different fields should cause cache misses", stats.getMisses(), equalTo(2L));
        assertThat("Should have no cache hits", stats.getHits(), equalTo(0L));

        // Verify wildcardQuery was called on BOTH field types (no cache sharing between fields)
        verify(fieldType1, times(1)).wildcardQuery(eq("server-*"), any(), anyBoolean(), any());
        verify(fieldType2, times(1)).wildcardQuery(eq("server-*"), any(), anyBoolean(), any());
    }

    /**
     * Test 5: Verify case sensitivity affects caching
     */
    public void testCaseSensitivityAffectsCaching() throws IOException {

        QueryShardContext context = createMockContext();

        // Same pattern, different case sensitivity settings
        CachedWildcardQueryBuilder builder1 = new CachedWildcardQueryBuilder("field1", "server-*");
        builder1.caseInsensitive(false);
        Query query1 = builder1.toQuery(context);

        CachedWildcardQueryBuilder builder2 = new CachedWildcardQueryBuilder("field1", "server-*");
        builder2.caseInsensitive(true);
        Query query2 = builder2.toQuery(context);

        // Get stats
        Cache.CacheStats stats = CachedWildcardQueryBuilder.getCacheStats();

        // Both should be cache misses (different case sensitivity = different cache keys)
        assertThat("Different case sensitivity should cause cache misses", stats.getMisses(), equalTo(2L));

        // Verify wildcardQuery was called twice (different case sensitivity creates different cache entries)
        verify(fieldType1, times(2)).wildcardQuery(eq("server-*"), any(), anyBoolean(), any());
    }

    /**
     * Test 6: Verify cache size no increase with default settings and increases as new patterns are added
     */
    public void testCacheSizeIncreases() throws IOException {

        long initialSize = CachedWildcardQueryBuilder.getCacheSize();
        QueryShardContext context = createMockContextWithDefaultSettings();
        assertThat("Cache should be empty initially", initialSize, equalTo(0L));
        // Add 3 different patterns
        new CachedWildcardQueryBuilder("field1", "pattern1*").toQuery(context);
        assertThat("Cache size should be 1", CachedWildcardQueryBuilder.getCacheSize(), equalTo(0L));

        new CachedWildcardQueryBuilder("field1", "pattern2*").toQuery(context);
        assertThat("Cache size should be 2", CachedWildcardQueryBuilder.getCacheSize(), equalTo(0L));

        new CachedWildcardQueryBuilder("field1", "pattern3*").toQuery(context);
        assertThat("Cache size should be 3", CachedWildcardQueryBuilder.getCacheSize(), equalTo(0L));

        context = createMockContext();
        // Add 3 different patterns
        new CachedWildcardQueryBuilder("field1", "pattern1*").toQuery(context);
        assertThat("Cache size should be 1", CachedWildcardQueryBuilder.getCacheSize(), equalTo(1L));

        new CachedWildcardQueryBuilder("field1", "pattern2*").toQuery(context);
        assertThat("Cache size should be 2", CachedWildcardQueryBuilder.getCacheSize(), equalTo(2L));

        new CachedWildcardQueryBuilder("field1", "pattern3*").toQuery(context);
        assertThat("Cache size should be 3", CachedWildcardQueryBuilder.getCacheSize(), equalTo(3L));
    }

    /**
     * Test 6b: Verify BiConsumer handles updates to single setting correctly
     * When only one setting is updated, BiConsumer should receive the new value for that setting
     * and the current value for the other setting.
     */
    public void testSingleSettingUpdateWithBiConsumer() throws IOException {
        // Start with initial cache (max_size=1000)
        QueryShardContext context1 = createMockContextWithCacheSize(1000);

        // Add 5 entries
        for (int i = 0; i < 5; i++) {
            new CachedWildcardQueryBuilder("field1", "pattern" + i + "*").toQuery(context1);
        }
        assertThat("Cache should hold 5 entries", CachedWildcardQueryBuilder.getCacheSize(), equalTo(5L));

        // Simulate updating ONLY max_size (not expiration)
        // The BiConsumer should receive: newMaxSize=2, currentExpiration (unchanged)
        QueryShardContext context2 = createMockContextWithCacheSize(2);

        // Add 2 more entries
        new CachedWildcardQueryBuilder("field1", "new1*").toQuery(context2);
        new CachedWildcardQueryBuilder("field1", "new2*").toQuery(context2);

        // With new maxSize=2, cache should evict old entries and hold at most 2
        assertThat("Cache size should respect new maxSize limit", CachedWildcardQueryBuilder.getCacheSize(), equalTo(2L));
    }

    /**
     * Test 7: Verify clearCache() empties the cache
     */
    public void testClearCache() throws IOException {
        QueryShardContext context = createMockContext();

        // Add some entries
        new CachedWildcardQueryBuilder("field1", "pattern1*").toQuery(context);
        new CachedWildcardQueryBuilder("field1", "pattern2*").toQuery(context);

        assertThat("Cache should have entries", CachedWildcardQueryBuilder.getCacheSize(), greaterThan(0L));

        // Clear cache
        CachedWildcardQueryBuilder.clearCache();

        assertThat("Cache should be empty after clear", CachedWildcardQueryBuilder.getCacheSize(), equalTo(0L));
    }

    /**
     * Test 8: Verify cache statistics are accurate
     */
    public void testCacheStatistics() throws IOException {

        QueryShardContext context = createMockContext();

        // Execute: Miss, Miss, Hit, Miss, Hit, Hit
        new CachedWildcardQueryBuilder("field1", "pattern1*").toQuery(context); // Miss
        new CachedWildcardQueryBuilder("field1", "pattern2*").toQuery(context); // Miss
        new CachedWildcardQueryBuilder("field1", "pattern1*").toQuery(context); // Hit
        new CachedWildcardQueryBuilder("field1", "pattern3*").toQuery(context); // Miss
        new CachedWildcardQueryBuilder("field1", "pattern1*").toQuery(context); // Hit
        new CachedWildcardQueryBuilder("field1", "pattern2*").toQuery(context); // Hit

        Cache.CacheStats stats = CachedWildcardQueryBuilder.getCacheStats();

        assertThat("Should have 3 cache misses", stats.getMisses(), equalTo(3L));
        assertThat("Should have 3 cache hits", stats.getHits(), equalTo(3L));
        assertThat("Cache size should be 3", CachedWildcardQueryBuilder.getCacheSize(), equalTo(3L));

        // Verify wildcardQuery was called only 3 times (for the 3 misses, not for the 3 hits)
        verify(fieldType1, times(1)).wildcardQuery(eq("pattern1*"), any(), anyBoolean(), any());
        verify(fieldType1, times(1)).wildcardQuery(eq("pattern2*"), any(), anyBoolean(), any());
        verify(fieldType1, times(1)).wildcardQuery(eq("pattern3*"), any(), anyBoolean(), any());
        // Total: 3 calls for 6 query executions - proving cache effectiveness!
        verify(fieldType1, times(3)).wildcardQuery(anyString(), any(), anyBoolean(), any());
    }

    /**
     * Test 9: Verify rewrite method affects cache key
     */
    public void testRewriteMethodAffectsCaching() throws IOException {

        QueryShardContext context = createMockContext();

        // Same pattern, different rewrite methods
        CachedWildcardQueryBuilder builder1 = new CachedWildcardQueryBuilder("field1", "server-*");
        builder1.rewrite("constant_score");
        Query query1 = builder1.toQuery(context);

        CachedWildcardQueryBuilder builder2 = new CachedWildcardQueryBuilder("field1", "server-*");
        builder2.rewrite("scoring_boolean");
        Query query2 = builder2.toQuery(context);

        CachedWildcardQueryBuilder builder3 = new CachedWildcardQueryBuilder("field1", "server-*");
        builder3.rewrite("constant_score");
        Query query3 = builder3.toQuery(context);

        Cache.CacheStats stats = CachedWildcardQueryBuilder.getCacheStats();

        // Should have 2 misses (two different rewrite methods) and 1 hit (same rewrite method)
        assertThat("Should have 2 cache misses", stats.getMisses(), equalTo(2L));
        assertThat("Should have 1 cache hit", stats.getHits(), equalTo(1L));

        // query1 and query3 should be same object (same rewrite method)
        assertThat("Same rewrite method should return same Query object", query3, sameInstance(query1));

        // Verify wildcardQuery was called only twice (not 3 times - third query was cache hit)
        verify(fieldType1, times(2)).wildcardQuery(eq("server-*"), any(), anyBoolean(), any());
    }

    // ========================================
    // EXCEPTION HANDLING TESTS
    // ========================================

    /**
     * Test 13: Verify exception when field doesn't exist in mapping
     */
    public void testDoToQueryThrowsExceptionForNonExistentField() {
        QueryShardContext context = mock(QueryShardContext.class);
        when(context.allowExpensiveQueries()).thenReturn(true);

        // Return null for non-existent field
        when(context.fieldMapper("nonexistent")).thenReturn(null);

        CachedWildcardQueryBuilder builder = new CachedWildcardQueryBuilder("nonexistent", "pattern*");

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> { builder.toQuery(context); });

        assertThat(
            "Exception message should mention field name",
            exception.getMessage(),
            equalTo("Field [nonexistent] does not exist in mapping")
        );
    }

    /**
     * Test 14: Verify exception when expensive queries are not allowed
     */
    public void testDoToQueryThrowsExceptionWhenExpensiveQueriesDisabled() {
        QueryShardContext context = mock(QueryShardContext.class);
        when(context.allowExpensiveQueries()).thenReturn(false);

        MappedFieldType fieldType = new TextFieldMapper.TextFieldType("field1");
        when(context.fieldMapper("field1")).thenReturn(fieldType);

        CachedWildcardQueryBuilder builder = new CachedWildcardQueryBuilder("field1", "pattern*");

        // This should throw an exception from the parent WildcardQueryBuilder
        expectThrows(Exception.class, () -> { builder.toQuery(context); });
    }

    /**
     * Test 15: Verify cache isolation between different field types
     */
    public void testDifferentFieldTypesCacheSeparately() throws IOException {
        QueryShardContext context = createMockContext();

        // Create query on field1 (TextFieldType)
        CachedWildcardQueryBuilder builder1 = new CachedWildcardQueryBuilder("field1", "pattern*");
        Query query1 = builder1.toQuery(context);

        // Create query on field2 (also TextFieldType, but different instance)
        CachedWildcardQueryBuilder builder2 = new CachedWildcardQueryBuilder("field2", "pattern*");
        Query query2 = builder2.toQuery(context);

        Cache.CacheStats stats = CachedWildcardQueryBuilder.getCacheStats();

        // Both should be cache misses (different field names, even with same pattern)
        assertThat("Different fields should cause cache misses", stats.getMisses(), equalTo(2L));
        assertThat("Should have no cache hits", stats.getHits(), equalTo(0L));
    }

    /**
     * Test 16: Verify cache works with builder chaining
     */
    public void testCacheWorksWithBuilderChaining() throws IOException {
        QueryShardContext context = createMockContext();

        // Create query with chained builder methods
        CachedWildcardQueryBuilder builder1 = new CachedWildcardQueryBuilder("field1", "server-*");
        builder1.caseInsensitive(true).rewrite("constant_score");
        Query query1 = builder1.toQuery(context);

        // Create another query with same chained settings
        CachedWildcardQueryBuilder builder2 = new CachedWildcardQueryBuilder("field1", "server-*");
        builder2.caseInsensitive(true).rewrite("constant_score");
        Query query2 = builder2.toQuery(context);

        Cache.CacheStats stats = CachedWildcardQueryBuilder.getCacheStats();

        // First should be miss, second should be hit
        assertThat("Should have 1 cache miss", stats.getMisses(), equalTo(1L));
        assertThat("Should have 1 cache hit", stats.getHits(), equalTo(1L));

        // Should return same Query object
        assertThat("Should return same Query object from cache", query2, sameInstance(query1));
    }
}
