/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.common;

import org.opensearch.test.OpenSearchTestCase;

import java.time.Duration;

/**
 * Tests for M3Duration functionality.
 */
public class M3DurationTests extends OpenSearchTestCase {

    /**
     * Test M3Duration with seconds units.
     */
    public void testSecondsUnits() {
        assertEquals(Duration.ofSeconds(5), M3Duration.valueOf("5s"));
        assertEquals(Duration.ofSeconds(10), M3Duration.valueOf("10sec"));
        assertEquals(Duration.ofSeconds(30), M3Duration.valueOf("30seconds"));
        assertEquals(Duration.ofSeconds(1), M3Duration.valueOf("1s"));
    }

    /**
     * Test M3Duration with minutes units.
     */
    public void testMinutesUnits() {
        assertEquals(Duration.ofMinutes(2), M3Duration.valueOf("2m"));
        assertEquals(Duration.ofMinutes(15), M3Duration.valueOf("15min"));
        assertEquals(Duration.ofMinutes(1), M3Duration.valueOf("1minute"));
        assertEquals(Duration.ofMinutes(45), M3Duration.valueOf("45minutes"));
    }

    /**
     * Test M3Duration with hours units.
     */
    public void testHoursUnits() {
        assertEquals(Duration.ofHours(3), M3Duration.valueOf("3h"));
        assertEquals(Duration.ofHours(6), M3Duration.valueOf("6hr"));
        assertEquals(Duration.ofHours(1), M3Duration.valueOf("1hour"));
        assertEquals(Duration.ofHours(24), M3Duration.valueOf("24hours"));
    }

    /**
     * Test M3Duration with days units.
     */
    public void testDaysUnits() {
        assertEquals(Duration.ofDays(1), M3Duration.valueOf("1d"));
        assertEquals(Duration.ofDays(7), M3Duration.valueOf("7day"));
        assertEquals(Duration.ofDays(30), M3Duration.valueOf("30days"));
    }

    /**
     * Test M3Duration with weeks units.
     */
    public void testWeeksUnits() {
        assertEquals(Duration.ofDays(7), M3Duration.valueOf("1w"));
        assertEquals(Duration.ofDays(14), M3Duration.valueOf("2week"));
        assertEquals(Duration.ofDays(21), M3Duration.valueOf("3weeks"));
    }

    /**
     * Test M3Duration with months units.
     */
    public void testMonthsUnits() {
        assertEquals(Duration.ofDays(30), M3Duration.valueOf("1mon"));
        assertEquals(Duration.ofDays(60), M3Duration.valueOf("2month"));
        assertEquals(Duration.ofDays(90), M3Duration.valueOf("3months"));
    }

    /**
     * Test M3Duration with years units.
     */
    public void testYearsUnits() {
        assertEquals(Duration.ofDays(365), M3Duration.valueOf("1y"));
        assertEquals(Duration.ofDays(730), M3Duration.valueOf("2year"));
        assertEquals(Duration.ofDays(1095), M3Duration.valueOf("3years"));
    }

    /**
     * Test M3Duration with large numeric values.
     */
    public void testLargeValues() {
        assertEquals(Duration.ofSeconds(3600), M3Duration.valueOf("3600s"));
        assertEquals(Duration.ofMinutes(1440), M3Duration.valueOf("1440m"));
        assertEquals(Duration.ofHours(168), M3Duration.valueOf("168h"));
        assertEquals(Duration.ofDays(365), M3Duration.valueOf("365d"));
    }

    /**
     * Test M3Duration with whitespace handling.
     */
    public void testWhitespaceHandling() {
        assertEquals(Duration.ofMinutes(5), M3Duration.valueOf("  5m  "));
        assertEquals(Duration.ofHours(2), M3Duration.valueOf("2h "));
        assertEquals(Duration.ofSeconds(30), M3Duration.valueOf(" 30s"));
        assertEquals(Duration.ofDays(1), M3Duration.valueOf("1 d"));
    }

    /**
     * Test M3Duration with zero values.
     */
    public void testZeroValues() {
        assertEquals(Duration.ofSeconds(0), M3Duration.valueOf("0s"));
        assertEquals(Duration.ofMinutes(0), M3Duration.valueOf("0m"));
        assertEquals(Duration.ofHours(0), M3Duration.valueOf("0h"));
        assertEquals(Duration.ofDays(0), M3Duration.valueOf("0d"));
    }

    /**
     * Test M3Duration with null input.
     */
    public void testNullInput() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> M3Duration.valueOf(null));
        assertEquals("Duration string cannot be null", exception.getMessage());
    }

    /**
     * Test M3Duration with empty input.
     */
    public void testEmptyInput() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> M3Duration.valueOf(""));
        assertEquals("Duration string is empty", exception.getMessage());

        exception = assertThrows(IllegalArgumentException.class, () -> M3Duration.valueOf("   "));
        assertEquals("Duration string is empty", exception.getMessage());
    }

    /**
     * Test M3Duration with invalid numeric input.
     */
    public void testInvalidNumericInput() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> M3Duration.valueOf("s"));
        assertEquals("Duration must start with a number: s", exception.getMessage());

        exception = assertThrows(IllegalArgumentException.class, () -> M3Duration.valueOf("abcs"));
        assertEquals("Duration must start with a number: abcs", exception.getMessage());
    }

    /**
     * Test M3Duration with missing unit.
     */
    public void testMissingUnit() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> M3Duration.valueOf("5"));
        assertEquals("Missing time unit in duration: 5", exception.getMessage());

        exception = assertThrows(IllegalArgumentException.class, () -> M3Duration.valueOf("123  "));
        assertEquals("Missing time unit in duration: 123  ", exception.getMessage());
    }

    /**
     * Test M3Duration with invalid unit.
     */
    public void testInvalidUnit() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> M3Duration.valueOf("5x"));
        assertEquals("Invalid duration. Unknown time unit: x", exception.getMessage());

        exception = assertThrows(IllegalArgumentException.class, () -> M3Duration.valueOf("10milliseconds"));
        assertEquals("Invalid duration. Unknown time unit: milliseconds", exception.getMessage());

        exception = assertThrows(IllegalArgumentException.class, () -> M3Duration.valueOf("2centuries"));
        assertEquals("Invalid duration. Unknown time unit: centuries", exception.getMessage());
    }

    /**
     * Test M3Duration with numeric overflow.
     */
    public void testNumericOverflow() {
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> M3Duration.valueOf("999999999999999999999999999999s")
        );
        assertEquals("Invalid numeric value in duration: 999999999999999999999999999999s", exception.getMessage());
    }

    /**
     * Test M3Duration with floating point numbers.
     */
    public void testFloatingPointNumbers() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> M3Duration.valueOf("5.5s"));
        assertEquals("Invalid duration. Unknown time unit: .5s", exception.getMessage());

        exception = assertThrows(IllegalArgumentException.class, () -> M3Duration.valueOf("3.14m"));
        assertEquals("Invalid duration. Unknown time unit: .14m", exception.getMessage());
    }

    /**
     * Test M3Duration with mixed alphanumeric in number part.
     */
    public void testMixedAlphanumericNumber() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> M3Duration.valueOf("5a3s"));
        assertEquals("Invalid duration. Unknown time unit: a3s", exception.getMessage());
    }

    public void testNegativeDurations() {
        assertEquals(Duration.ofSeconds(10).negated(), M3Duration.valueOf("-10s"));
        assertEquals(Duration.ofMinutes(5).negated(), M3Duration.valueOf("-5m"));
        assertEquals(Duration.ofHours(2).negated(), M3Duration.valueOf("-2h"));
        assertEquals(Duration.ofDays(1).negated(), M3Duration.valueOf("-1d"));
    }
}
