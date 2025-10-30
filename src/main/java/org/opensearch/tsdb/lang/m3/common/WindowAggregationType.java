/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.common;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

/**
 * The different types of window aggregations supported in M3QL.
 * <p>
 * Used in window aggregation functions like moving, summarize, etc.
 * <p>
 * For PERCENTILE type, use {@link #withPercentile(float)} to create an instance with a percentile value.
 */
public class WindowAggregationType implements Writeable {

    public enum Type {
        SUM,
        AVG,
        MIN,
        MAX,
        MEDIAN,
        LAST,
        STDDEV,
        PERCENTILE
    }

    // Singleton instances for non-percentile types
    public static final WindowAggregationType SUM = new WindowAggregationType(Type.SUM, 0);
    public static final WindowAggregationType AVG = new WindowAggregationType(Type.AVG, 0);
    public static final WindowAggregationType MIN = new WindowAggregationType(Type.MIN, 0);
    public static final WindowAggregationType MAX = new WindowAggregationType(Type.MAX, 0);
    public static final WindowAggregationType MEDIAN = new WindowAggregationType(Type.MEDIAN, 0);
    public static final WindowAggregationType LAST = new WindowAggregationType(Type.LAST, 0);
    public static final WindowAggregationType STDDEV = new WindowAggregationType(Type.STDDEV, 0);

    private final Type type;
    private final float percentileValue;

    private WindowAggregationType(Type type, float percentileValue) {
        this.type = type;
        this.percentileValue = percentileValue;
    }

    /**
     * Create a PERCENTILE aggregation type with the specified percentile value.
     *
     * @param percentile the percentile value (0-100)
     * @return a new WindowAggregationType for the specified percentile
     * @throws IllegalArgumentException if percentile is not in range [0, 100]
     */
    public static WindowAggregationType withPercentile(float percentile) {
        if (percentile < 0 || percentile > 100) {
            throw new IllegalArgumentException("Percentile must be in range [0, 100], got: " + percentile);
        }
        return new WindowAggregationType(Type.PERCENTILE, percentile);
    }

    /**
     * Check if this is a PERCENTILE aggregation type.
     */
    public boolean isPercentile() {
        return type == Type.PERCENTILE;
    }

    /**
     * Get the internal type enum for switch statements.
     * External code should use equals() or isPercentile() for type checking when possible.
     */
    public Type getType() {
        return type;
    }

    /**
     * Get the percentile value (only valid for PERCENTILE type).
     *
     * @return the percentile value
     * @throws IllegalStateException if this is not a PERCENTILE type
     */
    public float getPercentileValue() {
        if (type != Type.PERCENTILE) {
            throw new IllegalStateException("getPercentileValue() only valid for PERCENTILE type");
        }
        return percentileValue;
    }

    /**
     * Parse a string representation to create a WindowAggregationType.
     * Supports: sum, avg, min, max, median, last, stddev, p0-p100 (e.g., p50, p95).
     *
     * @param aggType the string representation
     * @return the corresponding WindowAggregationType
     * @throws IllegalArgumentException if the string is invalid
     */
    public static WindowAggregationType fromString(String aggType) {
        if (aggType == null || aggType.isEmpty()) {
            throw new IllegalArgumentException("Aggregation type cannot be null or empty");
        }

        String lowerAggType = aggType.toLowerCase(Locale.ROOT);
        return switch (lowerAggType) {
            case "avg", "average" -> AVG;
            case "max", "maximum" -> MAX;
            case "median" -> MEDIAN;
            case "min", "minimum" -> MIN;
            case "sum" -> SUM;
            case "last" -> LAST;
            case "stddev" -> STDDEV;
            default -> {
                // Check if it's a percentile function (p0, p50, p90, p95, p99, etc.)
                if (lowerAggType.startsWith("p") && lowerAggType.length() > 1) {
                    try {
                        String percentileStr = lowerAggType.substring(1);
                        float percentile = Float.parseFloat(percentileStr);
                        yield withPercentile(percentile);
                    } catch (IllegalArgumentException e) {
                        // Fall through to error (includes NumberFormatException)
                    }
                }
                throw new IllegalArgumentException(
                    "Invalid window aggregation type: "
                        + aggType
                        + ". "
                        + "Supported: sum, avg, max, min, median, last, stddev, p0-p100 (e.g., p50, p95)"
                );
            }
        };
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(type);
        if (type == Type.PERCENTILE) {
            out.writeFloat(percentileValue);
        }
    }

    /**
     * Read a WindowAggregationType from stream input.
     */
    public static WindowAggregationType readFrom(StreamInput in) throws IOException {
        Type type = in.readEnum(Type.class);
        if (type == Type.PERCENTILE) {
            float percentileValue = in.readFloat();
            return withPercentile(percentileValue);
        }
        return switch (type) {
            case SUM -> SUM;
            case AVG -> AVG;
            case MIN -> MIN;
            case MAX -> MAX;
            case MEDIAN -> MEDIAN;
            case LAST -> LAST;
            case STDDEV -> STDDEV;
            default -> throw new IllegalStateException("Unexpected type: " + type);
        };
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        WindowAggregationType that = (WindowAggregationType) obj;
        return type == that.type && Float.compare(that.percentileValue, percentileValue) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, percentileValue);
    }

    @Override
    public String toString() {
        if (type == Type.PERCENTILE) {
            return "p" + percentileValue;
        }
        return type.name();
    }
}
