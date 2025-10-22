/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.common;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Simple class that converts a user input M3QL duration to a {@link Duration}.
 */
public class M3Duration {
    private static final Map<String, TimeUnit> SUFFIXES = createSuffixes();

    private static final Map<String, Integer> LARGE_SUFFIXES = createLargeSuffixes();

    private static Map<String, TimeUnit> createSuffixes() {
        Map<String, TimeUnit> suffixes = new HashMap<>();
        suffixes.put("s", TimeUnit.SECONDS);
        suffixes.put("sec", TimeUnit.SECONDS);
        suffixes.put("seconds", TimeUnit.SECONDS);
        suffixes.put("m", TimeUnit.MINUTES);
        suffixes.put("min", TimeUnit.MINUTES);
        suffixes.put("minute", TimeUnit.MINUTES);
        suffixes.put("minutes", TimeUnit.MINUTES);
        suffixes.put("h", TimeUnit.HOURS);
        suffixes.put("hr", TimeUnit.HOURS);
        suffixes.put("hour", TimeUnit.HOURS);
        suffixes.put("hours", TimeUnit.HOURS);
        suffixes.put("d", TimeUnit.DAYS);
        suffixes.put("day", TimeUnit.DAYS);
        suffixes.put("days", TimeUnit.DAYS);
        return suffixes;
    }

    private static Map<String, Integer> createLargeSuffixes() {
        Map<String, Integer> largeSuffixes = new HashMap<>();
        largeSuffixes.put("w", 7); // weeks to days
        largeSuffixes.put("week", 7);
        largeSuffixes.put("weeks", 7);
        largeSuffixes.put("mon", 30); // months to days (M3QL semantics)
        largeSuffixes.put("month", 30);
        largeSuffixes.put("months", 30);
        largeSuffixes.put("y", 365); // years to days (M3QL semantics)
        largeSuffixes.put("year", 365);
        largeSuffixes.put("years", 365);
        return largeSuffixes;
    }

    /**
     * Private constructor to prevent instantiation.
     */
    private M3Duration() {
        // Prevent instantiation
    }

    /**
     * Converts a human-readable duration string to a {@link Duration}.
     *
     * @param duration e.g. 5m, 2w
     * @return Duration representing the specified time
     */
    public static Duration valueOf(String duration) {
        if (duration == null) {
            throw new IllegalArgumentException("Duration string cannot be null");
        }

        String trimmed = duration.trim();
        if (trimmed.isEmpty()) {
            throw new IllegalArgumentException("Duration string is empty");
        }

        int len = trimmed.length();
        int index = 0;

        // Skip optional leading '-'
        if (trimmed.charAt(0) == '-') {
            index++;
        }

        // Extract the numeric part (only digits allowed)
        while (index < len && Character.isDigit(trimmed.charAt(index))) {
            index++;
        }

        if (index == 0) {
            throw new IllegalArgumentException("Duration must start with a number: " + duration);
        }

        String numberStr = trimmed.substring(0, index);
        long count;
        try {
            count = Long.parseLong(numberStr);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid numeric value in duration: " + duration, e);
        }

        // Extract the unit part (the rest of the string)
        String unitStr = trimmed.substring(index).trim();
        if (unitStr.isEmpty()) {
            throw new IllegalArgumentException("Missing time unit in duration: " + duration);
        }

        TimeUnit unit = SUFFIXES.get(unitStr);
        if (unit == null) {
            Integer daysMultiplier = LARGE_SUFFIXES.get(unitStr);
            if (daysMultiplier != null) {
                return Duration.ofDays(daysMultiplier * count);
            }
            throw new IllegalArgumentException("Invalid duration. Unknown time unit: " + unitStr);
        }

        return Duration.ofMillis(unit.toMillis(count));
    }
}
