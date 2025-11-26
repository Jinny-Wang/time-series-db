/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.benchmark;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.LongRange;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.opensearch.index.mapper.RangeFieldMapper;
import org.opensearch.tsdb.core.mapping.Constants;
import org.opensearch.tsdb.core.utils.TimestampRangeEncoding;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;

/**
 * Benchmark comparing timestamp filtering performance between different indexing and query strategies:
 *
 * BKD Tree (Point) Queries:
 * 1. benchmarkTwoLongPoints - Two separate LongPoint fields (min_timestamp and max_timestamp)
 * 2. benchmarkLongRangeField - Single LongRange field storing the timestamp range
 *
 * Doc Values Queries:
 * 3. benchmarkTwoNumericDocValues - Two separate NumericDocValuesField (min and max)
 * 4. benchmarkProductionDocValuesQuery - Production approach: BinaryDocValuesField with RangeType.LONG encoding
 *
 * Production Approach (TSDB):
 * - Indexing: LongRange (BKD tree) + BinaryDocValuesField (RangeType.LONG VarInt encoding)
 * - Querying: OpenSearch's standard QueryBuilders.rangeQuery() with long_range field mapping
 * - The field mapping ("timestamp_range": {"type": "long_range"}) enables OpenSearch's
 *   RangeQueryBuilder to create an IndexOrDocValuesQuery that adaptively chooses between
 *   BKD tree (for selective queries) and doc values (for dense queries).
 *
 * This benchmark simulates the TSDB use case where chunks are stored with
 * a timestamp range, and queries need to find chunks that intersect with
 * a query time range (the most common query pattern).
 *
 * The doc values queries are particularly useful for scenarios where many documents match
 * (high selectivity), as they can be more efficient than BKD tree traversal.
 */
@State(Scope.Benchmark)
@BenchmarkMode({ Mode.AverageTime })
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 10, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 10, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1, jvmArgs = { "-Xms4g", "-Xmx4g" })
public class TimestampFieldBenchmark {

    // Field names
    private static final String MIN_TIMESTAMP = "min_timestamp";
    private static final String MAX_TIMESTAMP = "max_timestamp";
    private static final String TIMESTAMP_RANGE = "timestamp_range";

    @Param({ "10000", "100000", "1000000" })
    private int numDocuments;

    @Param({ "SMALL", "MEDIUM", "LARGE" })
    private QueryRangeSize queryRangeSize;

    @Param({ "SELECTIVE", "BROAD" })
    private QuerySelectivity querySelectivity;

    private Path tempDirTwoPoints;
    private Path tempDirRangeField;
    private Path tempDirProduction;
    private Directory directoryTwoPoints;
    private Directory directoryRangeField;
    private Directory directoryProduction;
    private DirectoryReader readerTwoPoints;
    private DirectoryReader readerRangeField;
    private DirectoryReader readerProduction;
    private IndexSearcher searcherTwoPoints;
    private IndexSearcher searcherRangeField;
    private IndexSearcher searcherProduction;

    // Query parameters - set during setup based on queryRangeSize and querySelectivity
    private long queryStart;
    private long queryEnd;

    /**
     * Size of the query time range relative to document chunk ranges
     */
    public enum QueryRangeSize {
        SMALL(60_000L),      // 1 minute - smaller than typical chunks
        MEDIUM(3600_000L),   // 1 hour - similar to typical chunks
        LARGE(86400_000L);   // 1 day - larger than typical chunks

        private final long rangeMillis;

        QueryRangeSize(long rangeMillis) {
            this.rangeMillis = rangeMillis;
        }

        public long getRangeMillis() {
            return rangeMillis;
        }
    }

    /**
     * Selectivity of the query - how many documents match
     */
    public enum QuerySelectivity {
        SELECTIVE,  // ~10% of documents match
        BROAD       // ~50% of documents match
    }

    @Setup(Level.Trial)
    public void setup() throws IOException {
        tempDirTwoPoints = Files.createTempDirectory("jmh-two-points-");
        tempDirRangeField = Files.createTempDirectory("jmh-range-field-");
        tempDirProduction = Files.createTempDirectory("jmh-production-");

        directoryTwoPoints = new MMapDirectory(tempDirTwoPoints);
        directoryRangeField = new MMapDirectory(tempDirRangeField);
        directoryProduction = new MMapDirectory(tempDirProduction);

        // Index documents with all field types
        indexDocumentsWithTwoPoints();
        indexDocumentsWithRangeField();
        indexDocumentsWithProductionApproach();

        // Open readers and searchers
        readerTwoPoints = DirectoryReader.open(directoryTwoPoints);
        readerRangeField = DirectoryReader.open(directoryRangeField);
        readerProduction = DirectoryReader.open(directoryProduction);
        searcherTwoPoints = new IndexSearcher(readerTwoPoints);
        searcherRangeField = new IndexSearcher(readerRangeField);
        searcherProduction = new IndexSearcher(readerProduction);

        // Set up query parameters
        setupQueryParameters();
    }

    private void setupQueryParameters() {
        // Base time: 1 Jan 2024 00:00:00 GMT
        long baseTime = 1704067200000L;
        long totalTimeSpan = numDocuments * 3600_000L; // 1 hour per document on average

        // Calculate query start time based on selectivity
        switch (querySelectivity) {
            case SELECTIVE:
                // Query the middle 10% of the time range
                queryStart = baseTime + (long) (totalTimeSpan * 0.45);
                break;
            case BROAD:
                // Query the middle 50% of the time range
                queryStart = baseTime + (long) (totalTimeSpan * 0.25);
                break;
        }

        queryEnd = queryStart + queryRangeSize.getRangeMillis();
    }

    private void indexDocumentsWithTwoPoints() throws IOException {
        IndexWriterConfig config = new IndexWriterConfig();
        try (IndexWriter writer = new IndexWriter(directoryTwoPoints, config)) {
            long baseTimestamp = 1704067200000L; // 1 Jan 2024 00:00:00 GMT
            long chunkDuration = 3600_000L; // 1 hour chunks

            for (int i = 0; i < numDocuments; i++) {
                Document doc = new Document();

                // Simulate chunks with varying durations (30 min to 2 hours)
                long minTs = baseTimestamp + (i * chunkDuration);
                long duration = chunkDuration / 2 + (i % 3) * (chunkDuration / 2); // 30min, 1h, or 1.5h
                long maxTs = minTs + duration;

                // Add two separate LongPoint fields
                doc.add(new LongPoint(MIN_TIMESTAMP, minTs));
                doc.add(new NumericDocValuesField(MIN_TIMESTAMP, minTs));
                doc.add(new LongPoint(MAX_TIMESTAMP, maxTs));
                doc.add(new NumericDocValuesField(MAX_TIMESTAMP, maxTs));

                writer.addDocument(doc);
            }
            writer.commit();
        }
    }

    private void indexDocumentsWithRangeField() throws IOException {
        IndexWriterConfig config = new IndexWriterConfig();
        try (IndexWriter writer = new IndexWriter(directoryRangeField, config)) {
            long baseTimestamp = 1704067200000L; // 1 Jan 2024 00:00:00 GMT
            long chunkDuration = 3600_000L; // 1 hour chunks

            for (int i = 0; i < numDocuments; i++) {
                Document doc = new Document();

                // Simulate chunks with varying durations (30 min to 2 hours)
                long minTs = baseTimestamp + (i * chunkDuration);
                long duration = chunkDuration / 2 + (i % 3) * (chunkDuration / 2); // 30min, 1h, or 1.5h
                long maxTs = minTs + duration;

                // Add LongRange field
                doc.add(new LongRange(TIMESTAMP_RANGE, new long[] { minTs }, new long[] { maxTs }));
                // Also add doc values for compatibility
                doc.add(new NumericDocValuesField(MIN_TIMESTAMP, minTs));
                doc.add(new NumericDocValuesField(MAX_TIMESTAMP, maxTs));

                writer.addDocument(doc);
            }
            writer.commit();
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        if (readerTwoPoints != null) {
            readerTwoPoints.close();
        }
        if (readerRangeField != null) {
            readerRangeField.close();
        }
        if (readerProduction != null) {
            readerProduction.close();
        }
        if (directoryTwoPoints != null) {
            directoryTwoPoints.close();
        }
        if (directoryRangeField != null) {
            directoryRangeField.close();
        }
        if (directoryProduction != null) {
            directoryProduction.close();
        }
        deleteDirectory(tempDirTwoPoints);
        deleteDirectory(tempDirRangeField);
        deleteDirectory(tempDirProduction);
    }

    private void deleteDirectory(Path directory) throws IOException {
        if (directory != null && Files.exists(directory)) {
            Files.walk(directory).sorted(Comparator.reverseOrder()).forEach(path -> {
                try {
                    Files.delete(path);
                } catch (IOException e) {
                    System.err.println("Failed to delete " + path + ": " + e.getMessage());
                }
            });
        }
    }

    /**
     * Benchmark the current approach: Two separate LongPoint fields
     * Query: Find chunks where min_timestamp &lt; queryEnd AND max_timestamp &gt;= queryStart
     * This implements the [start, end) semantics used in M3QL
     */
    @Benchmark
    public void benchmarkTwoLongPoints(Blackhole bh) throws IOException {
        // Build query: min_timestamp < queryEnd AND max_timestamp >= queryStart
        // This finds all chunks that intersect with the query range
        BooleanQuery.Builder boolBuilder = new BooleanQuery.Builder();
        boolBuilder.add(LongPoint.newRangeQuery(MIN_TIMESTAMP, Long.MIN_VALUE, queryEnd - 1), BooleanClause.Occur.FILTER);
        boolBuilder.add(LongPoint.newRangeQuery(MAX_TIMESTAMP, queryStart, Long.MAX_VALUE), BooleanClause.Occur.FILTER);
        Query query = boolBuilder.build();

        TotalHitCountCollector collector = new TotalHitCountCollector();
        searcherTwoPoints.search(query, collector);
        bh.consume(collector.getTotalHits());
    }

    /**
     * Benchmark the alternative approach: Single LongRange field
     * Query: Find ranges that intersect with the query range
     */
    @Benchmark
    public void benchmarkLongRangeField(Blackhole bh) throws IOException {
        // Use LongRange.newIntersectsQuery to find all ranges that intersect with [queryStart, queryEnd]
        Query query = LongRange.newIntersectsQuery(TIMESTAMP_RANGE, new long[] { queryStart }, new long[] { queryEnd });

        TotalHitCountCollector collector = new TotalHitCountCollector();
        searcherRangeField.search(query, collector);
        bh.consume(collector.getTotalHits());
    }

    /**
     * Benchmark doc values query with TWO separate NumericDocValuesField
     * This shows the alternative of querying two separate doc values fields
     * Reuses the data from benchmarkTwoLongPoints (which has both LongPoint and NumericDocValuesField)
     */
    @Benchmark
    public void benchmarkTwoNumericDocValues(Blackhole bh) throws IOException {
        // Use a custom doc values query that reads two separate NumericDocValues fields
        Query query = new TwoNumericDocValuesRangeQuery(MIN_TIMESTAMP, MAX_TIMESTAMP, queryStart, queryEnd);

        TotalHitCountCollector collector = new TotalHitCountCollector();
        searcherTwoPoints.search(query, collector);
        bh.consume(collector.getTotalHits());
    }

    /**
     * Benchmark the production doc values query path with RangeType.LONG encoding.
     *
     * In production, OpenSearch's RangeQueryBuilder creates an IndexOrDocValuesQuery that
     * adaptively chooses between BKD tree and doc values based on cost estimation.
     * This benchmark specifically tests the doc values path performance with our encoding.
     *
     * Indexing: LongRange (BKD tree) + BinaryDocValuesField (RangeType.LONG VarInt encoding)
     * Query: Doc values scan using RangeType.LONG decoding
     *
     * Note: In production, we use QueryBuilders.rangeQuery() with long_range field mapping,
     * which OpenSearch handles automatically. This benchmark uses a custom query to
     * isolate and measure the doc values decoding performance.
     */
    @Benchmark
    public void benchmarkProductionDocValuesQuery(Blackhole bh) throws IOException {
        Query query = new ProductionRangeDocValuesQuery(Constants.IndexSchema.TIMESTAMP_RANGE, queryStart, queryEnd);

        TotalHitCountCollector collector = new TotalHitCountCollector();
        searcherProduction.search(query, collector);
        bh.consume(collector.getTotalHits());
    }

    private void indexDocumentsWithProductionApproach() throws IOException {
        IndexWriterConfig config = new IndexWriterConfig();
        try (IndexWriter writer = new IndexWriter(directoryProduction, config)) {
            long baseTimestamp = 1704067200000L; // 1 Jan 2024 00:00:00 GMT
            long chunkDuration = 3600_000L; // 1 hour chunks

            for (int i = 0; i < numDocuments; i++) {
                Document doc = new Document();

                // Simulate chunks with varying durations (30 min to 2 hours)
                long minTs = baseTimestamp + (i * chunkDuration);
                long duration = chunkDuration / 2 + (i % 3) * (chunkDuration / 2); // 30min, 1h, or 1.5h
                long maxTs = minTs + duration;

                // Production approach: LongRange for BKD tree + BinaryDocValuesField for doc values
                doc.add(new LongRange(Constants.IndexSchema.TIMESTAMP_RANGE, new long[] { minTs }, new long[] { maxTs }));
                doc.add(new BinaryDocValuesField(Constants.IndexSchema.TIMESTAMP_RANGE, TimestampRangeEncoding.encodeRange(minTs, maxTs)));

                writer.addDocument(doc);
            }
            writer.commit();
        }
    }

    /**
     * Custom query that reads two separate NumericDocValues fields to check timestamp range intersection.
     * This is used for benchmarking comparison against the single BinaryDocValuesField approach.
     */
    static class TwoNumericDocValuesRangeQuery extends Query {
        private final String minField;
        private final String maxField;
        private final long queryStart;
        private final long queryEnd;

        TwoNumericDocValuesRangeQuery(String minField, String maxField, long queryStart, long queryEnd) {
            this.minField = minField;
            this.maxField = maxField;
            this.queryStart = queryStart;
            this.queryEnd = queryEnd;
        }

        @Override
        public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
            return new ConstantScoreWeight(this, boost) {
                @Override
                public boolean isCacheable(LeafReaderContext ctx) {
                    return DocValues.isCacheable(ctx, minField) && DocValues.isCacheable(ctx, maxField);
                }

                @Override
                public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                    final long cost = context.reader().maxDoc();

                    return new ScorerSupplier() {
                        @Override
                        public Scorer get(long leadCost) throws IOException {
                            final NumericDocValues minValues = DocValues.getNumeric(context.reader(), minField);
                            final NumericDocValues maxValues = DocValues.getNumeric(context.reader(), maxField);
                            final DocIdSetIterator approximation = DocIdSetIterator.all(context.reader().maxDoc());

                            final TwoPhaseIterator twoPhase = new TwoPhaseIterator(approximation) {
                                @Override
                                public boolean matches() throws IOException {
                                    int docID = approximation.docID();

                                    // Read both doc values
                                    if (minValues.advanceExact(docID) && maxValues.advanceExact(docID)) {
                                        long docMin = minValues.longValue();
                                        long docMax = maxValues.longValue();

                                        // Check intersection: [queryStart, queryEnd) intersects [docMin, docMax]
                                        return docMax >= queryStart && docMin < queryEnd;
                                    }

                                    return false;
                                }

                                @Override
                                public float matchCost() {
                                    // 2x advanceExact (2) + 2x longValue (2) + 2 comparisons (1) = ~5
                                    // Note: No decoding overhead like BinaryDocValues, but two field accesses
                                    return 5;
                                }
                            };

                            return new ConstantScoreScorer(score(), scoreMode, twoPhase);
                        }

                        @Override
                        public long cost() {
                            return cost;
                        }
                    };
                }
            };
        }

        @Override
        public String toString(String field) {
            return "TwoNumericDocValuesRangeQuery(min="
                + minField
                + " max="
                + maxField
                + " range=["
                + queryStart
                + " TO "
                + queryEnd
                + "))";
        }

        @Override
        public boolean equals(Object obj) {
            if (!sameClassAs(obj)) return false;
            TwoNumericDocValuesRangeQuery other = (TwoNumericDocValuesRangeQuery) obj;
            return minField.equals(other.minField)
                && maxField.equals(other.maxField)
                && queryStart == other.queryStart
                && queryEnd == other.queryEnd;
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(minField, maxField, queryStart, queryEnd);
        }

        @Override
        public void visit(QueryVisitor visitor) {
            if (visitor.acceptField(minField) && visitor.acceptField(maxField)) {
                visitor.visitLeaf(this);
            }
        }
    }

    /**
     * Custom query that reads BinaryDocValues with RangeType.LONG encoding (production approach).
     * Tests the performance of the production doc values query path.
     */
    static class ProductionRangeDocValuesQuery extends Query {
        private final String rangeField;
        private final long queryStart;
        private final long queryEnd;

        ProductionRangeDocValuesQuery(String rangeField, long queryStart, long queryEnd) {
            this.rangeField = rangeField;
            this.queryStart = queryStart;
            this.queryEnd = queryEnd;
        }

        @Override
        public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
            return new ConstantScoreWeight(this, boost) {
                @Override
                public boolean isCacheable(LeafReaderContext ctx) {
                    return DocValues.isCacheable(ctx, rangeField);
                }

                @Override
                public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                    final long cost = context.reader().maxDoc();

                    return new ScorerSupplier() {
                        @Override
                        public Scorer get(long leadCost) throws IOException {
                            final org.apache.lucene.index.BinaryDocValues rangeValues = DocValues.getBinary(context.reader(), rangeField);
                            final DocIdSetIterator approximation = DocIdSetIterator.all(context.reader().maxDoc());

                            final TwoPhaseIterator twoPhase = new TwoPhaseIterator(approximation) {
                                @Override
                                public boolean matches() throws IOException {
                                    int docID = approximation.docID();

                                    if (rangeValues.advanceExact(docID)) {
                                        org.apache.lucene.util.BytesRef encodedRange = rangeValues.binaryValue();

                                        // Decode using RangeType.LONG (production approach)
                                        RangeFieldMapper.Range decodedRange = TimestampRangeEncoding.decodeRange(encodedRange);
                                        long docMin = (long) decodedRange.getFrom();
                                        long docMax = (long) decodedRange.getTo();

                                        // Check intersection: [queryStart, queryEnd) intersects [docMin, docMax]
                                        return docMax >= queryStart && docMin < queryEnd;
                                    }

                                    return false;
                                }

                                @Override
                                public float matchCost() {
                                    // advanceExact (1) + binaryValue (1) + decodeRange (3) + 2 comparisons (1) = ~6
                                    return 6;
                                }
                            };

                            return new ConstantScoreScorer(score(), scoreMode, twoPhase);
                        }

                        @Override
                        public long cost() {
                            return cost;
                        }
                    };
                }
            };
        }

        @Override
        public String toString(String field) {
            return "ProductionRangeDocValuesQuery(field=" + rangeField + " range=[" + queryStart + " TO " + queryEnd + "))";
        }

        @Override
        public boolean equals(Object obj) {
            if (!sameClassAs(obj)) return false;
            ProductionRangeDocValuesQuery other = (ProductionRangeDocValuesQuery) obj;
            return rangeField.equals(other.rangeField) && queryStart == other.queryStart && queryEnd == other.queryEnd;
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(rangeField, queryStart, queryEnd);
        }

        @Override
        public void visit(QueryVisitor visitor) {
            if (visitor.acceptField(rangeField)) {
                visitor.visitLeaf(this);
            }
        }
    }
}
