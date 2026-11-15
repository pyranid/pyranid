/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.time.DateTimeException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

class SqlIntervalTextTests {
	private static final BigInteger HOUR = BigInteger.valueOf(3_600_000_000L);
	private static final BigInteger MINUTE = BigInteger.valueOf(60_000_000L);
	private static final BigInteger SECOND = BigInteger.valueOf(1_000_000L);

	@Test
	void postgresOutputStylesPreserveIndependentSignsAndSqlStandardWholeValueSigns() {
		SqlInterval mixed = SqlInterval.of(14, -3, Duration.ofSeconds(14706, 789000000));
		for (String text : List.of("1 year 2 mons -3 days +04:05:06.789", "@ 1 year 2 mons -3 days 4 hours 5 mins 6.789 secs",
				"+1-2 -3 +4:05:06.789", "P1Y2M-3DT4H5M6.789S"))
			assertEquals(mixed, SqlIntervalText.parsePostgres(text), text);
		for (String text : List.of("-3 days -04:05:06.789", "@ 3 days 4 hours 5 mins 6.789 secs ago",
				"-3 4:05:06.789", "P-3DT-4H-5M-6.789S"))
			assertEquals(SqlInterval.of(0, -3, Duration.ofSeconds(-14707, 211000000)), SqlIntervalText.parsePostgres(text), text);
		assertEquals(SqlInterval.of(-14, 3, Duration.ofMinutes(-90)),
				SqlIntervalText.parsePostgres("@ 1 year 2 mons -3 days 1 hour 30 mins ago"));
		assertEquals(SqlInterval.of(-14, 0, Duration.ZERO), SqlIntervalText.parsePostgres("-1-2"));
		assertEquals(SqlInterval.of(14, 0, Duration.ZERO), SqlIntervalText.parsePostgres("1-2"));
		assertEquals(SqlInterval.of(Duration.ofNanos(-1000)), SqlIntervalText.parsePostgres("-00:00:00.000001"));
		for (String zero : List.of("0", "00:00:00", "@ 0", "PT0S"))
			assertSame(SqlInterval.ZERO, SqlIntervalText.parsePostgres(zero));
	}

	@Test
	void duckOutputUsesItsOwnMonthNamesAndDoesNotFoldTimeIntoDays() {
		assertEquals(SqlInterval.of(14, -3, Duration.ofMinutes(-90)),
				SqlIntervalText.parseDuckDb("1 year 2 months -3 days -01:30:00"));
		assertEquals(SqlInterval.of(Duration.ofHours(36)), SqlIntervalText.parseDuckDb("36:00:00"));
		assertEquals(SqlInterval.of(0, 1, Duration.ZERO), SqlIntervalText.parseDuckDb("1 day"));
		assertSame(SqlInterval.ZERO, SqlIntervalText.parseDuckDb("00:00:00"));
		assertThrows(DateTimeException.class, () -> SqlIntervalText.parseDuckDb("1 mon"));
		assertThrows(DateTimeException.class, () -> SqlIntervalText.parsePostgres("1 month"));
	}

	@Test
	void exactMicrosecondConversionIncludesNegativeMinimumDespiteIntermediateScalingUnderflow() {
		Duration minimum = Duration.ofSeconds(-9223372036855L, 224192000);
		Duration maximum = Duration.ofSeconds(9223372036854L, 775807000);
		assertEquals(minimum, SqlIntervalText.fromMicroseconds(Long.MIN_VALUE));
		assertEquals(maximum, SqlIntervalText.fromMicroseconds(Long.MAX_VALUE));
		assertEquals(Long.MIN_VALUE, SqlIntervalText.toMicroseconds(minimum));
		assertEquals(Long.MAX_VALUE, SqlIntervalText.toMicroseconds(maximum));
		assertThrows(DateTimeException.class, () -> SqlIntervalText.toMicroseconds(minimum.minusNanos(1000)));
		assertThrows(DateTimeException.class, () -> SqlIntervalText.toMicroseconds(maximum.plusNanos(1000)));
		for (long micros : new long[]{Long.MIN_VALUE, Long.MIN_VALUE + 1, -1_000_001, -1_000_000, -1, 0, 1,
				999_999, 1_000_000, Long.MAX_VALUE - 1, Long.MAX_VALUE})
			assertEquals(micros, SqlIntervalText.toMicroseconds(SqlIntervalText.fromMicroseconds(micros)));
		for (Duration fractional : List.of(Duration.ofNanos(1), Duration.ofNanos(-1), Duration.ofNanos(1500),
				Duration.ofSeconds(-1, 1), Duration.ofSeconds(1, 999)))
			assertThrows(DateTimeException.class, () -> SqlIntervalText.toMicroseconds(fractional));
	}

	@Test
	void postgresInfinityIsOnlyTheReservedTripleAndDuckCanRepresentThoseFiniteFields() {
		for (boolean positive : new boolean[]{false, true}) {
			int extreme = positive ? Integer.MAX_VALUE : Integer.MIN_VALUE;
			long micros = positive ? Long.MAX_VALUE : Long.MIN_VALUE;
			SqlInterval reserved = SqlInterval.of(extreme, extreme, SqlIntervalText.fromMicroseconds(micros));
			assertThrows(DateTimeException.class, () -> SqlIntervalText.rejectPostgresInfinity(reserved));
			assertThrows(DateTimeException.class, () -> SqlIntervalText.formatPostgres(reserved));
			for (String text : List.of(postgres(reserved), verbose(reserved), sqlStandard(reserved), reserved.toString()))
				assertThrows(DateTimeException.class, () -> SqlIntervalText.parsePostgres(text), text);
			assertEquals(reserved, SqlIntervalText.parseDuckDb(words(reserved, true)));
			for (SqlInterval finite : List.of(SqlInterval.of(extreme, 0, Duration.ZERO), SqlInterval.of(0, extreme, Duration.ZERO),
					SqlInterval.of(SqlIntervalText.fromMicroseconds(micros)), SqlInterval.of(extreme, extreme, Duration.ZERO))) {
				assertDoesNotThrow(() -> SqlIntervalText.rejectPostgresInfinity(finite));
				assertEquals(finite, SqlIntervalText.parsePostgres(SqlIntervalText.formatPostgres(finite)));
			}
		}
		for (String infinity : List.of("infinity", "-infinity")) {
			assertThrows(DateTimeException.class, () -> SqlIntervalText.parsePostgres(infinity));
			assertThrows(DateTimeException.class, () -> SqlIntervalText.parseDuckDb(infinity));
		}
	}

	@Test
	void everyExplicitSignedBoundaryIsCheckedAcrossAllTextStyles() {
		int[] months = {Integer.MIN_VALUE, Integer.MIN_VALUE + 1, -13, -1, 0, 1, 13, Integer.MAX_VALUE - 1, Integer.MAX_VALUE};
		int[] days = {Integer.MIN_VALUE, -1, 0, 1, Integer.MAX_VALUE};
		long[] micros = {Long.MIN_VALUE, Long.MIN_VALUE + 1, -1_000_001, -1, 0, 1, 1_000_001, Long.MAX_VALUE - 1, Long.MAX_VALUE};
		for (int month : months) for (int day : days) for (long micro : micros)
			assertRepresentations(SqlInterval.of(month, day, SqlIntervalText.fromMicroseconds(micro)));
	}

	@Test
	void fixedSeedGeneratedValidOutputsRetainFieldsRatherThanSqlEquality() {
		Random random = new Random(519423849L);
		for (int index = 0; index < 1500; ++index)
			assertRepresentations(SqlInterval.of(random.nextInt(), random.nextInt(), SqlIntervalText.fromMicroseconds(random.nextLong())));
	}

	@Test
	void malformedOrOverflowedTextCannotBeAcceptedAsAPrefixOrNormalizedByGuessing() {
		List<String> invalid = List.of("", " ", "P", "PT", "P1DT", "P1M99", "PT1S99", "PT1Sjunk", "P1M1Y", "P1D1D",
				"PT1H1H", "P1M2M", "P1.5D", "P1W", "P2147483648M", "P-2147483649D", "PT2562047789H",
				"PT60M", "PT60S", "PT1.0000001S", "PT1e2S", "PT--1S", "P+M", "1 day junk", "1 day 2 days",
				"1 day 2 mons", "1 mon 2 years", "1", "00:60:00", "00:00:60", "00:00:00.0000001", "1:2:3",
				"1:02:03x", "1-12", "1-2 3 04:05:06", "+1-2 +3 04:05:06", "-1 -02:00:00", "1 02:00:00 extra",
				"@", "@1 day", "@ 1 day ago junk", "@ 1 sec 2 mins", "@ 1.0000001 secs", "@ 1 hour 60 mins",
				"@ 1 hour -60 secs", "@ 1 minute", "NaN", "Infinity", "0".repeat(513));
		for (String text : invalid)
			assertThrows(DateTimeException.class, () -> SqlIntervalText.parsePostgres(text), text);
		for (String text : List.of("", "P1D", "1 day junk", "1 day 2 days", "2 days 1 month", "1 mon", "1 hour",
				"1 month 0:00:60", "1 month 0:00:00.0000001", "2147483648 months", "-2147483649 days", "0".repeat(513)))
			assertThrows(DateTimeException.class, () -> SqlIntervalText.parseDuckDb(text), text);
	}

	@Test
	void codecsDoNotEchoMalformedRowValuesAndValidateBeforeFormatting() {
		String secret = "private-row-value";
		DateTimeException failure = assertThrows(DateTimeException.class, () -> SqlIntervalText.parsePostgres(secret));
		assertFalse(failure.getMessage().contains(secret));
		assertThrows(DateTimeException.class, () -> SqlIntervalText.formatPostgres(SqlInterval.of(Duration.ofNanos(1))));
		assertThrows(DateTimeException.class, () -> SqlIntervalText.formatDuckDb(SqlInterval.of(Duration.ofNanos(1))));
		assertEquals("14 months -3 days 0 seconds -1000 microseconds",
				SqlIntervalText.formatDuckDb(SqlInterval.of(14, -3, Duration.ofNanos(-1_000_000))));
	}

	@Test
	void postgresInputEncodingRetainsItsOwnCalendarFieldsAndIsoElapsedTime() {
		assertEquals("PT0S", SqlIntervalText.formatPostgres(SqlInterval.ZERO));
		assertEquals("P14M", SqlIntervalText.formatPostgres(SqlInterval.of(14, 0, Duration.ZERO)));
		assertEquals("P-3D", SqlIntervalText.formatPostgres(SqlInterval.of(0, -3, Duration.ZERO)));
		assertEquals("P14M-3DT4H5M6.789S",
				SqlIntervalText.formatPostgres(SqlInterval.of(14, -3, Duration.ofSeconds(14706, 789000000))));
		assertEquals("P-14M3DT-0.876544S",
				SqlIntervalText.formatPostgres(SqlInterval.of(-14, 3, Duration.ofSeconds(-1, 123456000))));
		assertEquals("PT-0.000001S", SqlIntervalText.formatPostgres(SqlInterval.of(Duration.ofNanos(-1000))));
		assertEquals("P-2147483648M2147483647D",
				SqlIntervalText.formatPostgres(SqlInterval.of(Integer.MIN_VALUE, Integer.MAX_VALUE, Duration.ZERO)));
		assertEquals("PT-2562047788H-54.775808S",
				SqlIntervalText.formatPostgres(SqlInterval.of(SqlIntervalText.fromMicroseconds(Long.MIN_VALUE))));
		assertEquals("PT2562047788H54.775807S",
				SqlIntervalText.formatPostgres(SqlInterval.of(SqlIntervalText.fromMicroseconds(Long.MAX_VALUE))));
	}

	@Test
	void duckLiteralSplitsElapsedSecondsAndMicrosWithoutAnUnrepresentableMagnitude() {
		assertEquals("0 months 0 days -9223372036854 seconds -775808 microseconds",
				SqlIntervalText.formatDuckDb(SqlInterval.of(SqlIntervalText.fromMicroseconds(Long.MIN_VALUE))));
		for (int month : new int[]{Integer.MIN_VALUE, 0, Integer.MAX_VALUE})
			for (int day : new int[]{Integer.MIN_VALUE, 0, Integer.MAX_VALUE})
				for (long micros : new long[]{Long.MIN_VALUE, Long.MIN_VALUE + 1, -1_000_001, -1, 0, 1, 1_000_001, Long.MAX_VALUE}) {
					String literal = SqlIntervalText.formatDuckDb(SqlInterval.of(month, day, SqlIntervalText.fromMicroseconds(micros)));
					String[] fields = literal.split(" ");
					assertEquals(month, Integer.parseInt(fields[0]));
					assertEquals(day, Integer.parseInt(fields[2]));
					long seconds = Long.parseLong(fields[4]), remainder = Long.parseLong(fields[6]);
					assertNotEquals(Long.MIN_VALUE, seconds);
					assertTrue(Math.abs(remainder) < 1_000_000);
					assertEquals(BigInteger.valueOf(micros), BigInteger.valueOf(seconds).multiply(SECOND).add(BigInteger.valueOf(remainder)));
				}
	}

	private static void assertRepresentations(SqlInterval value) {
		long micros = SqlIntervalText.toMicroseconds(value.getTime());
		boolean reserved = value.getMonths() == Integer.MAX_VALUE && value.getDays() == Integer.MAX_VALUE && micros == Long.MAX_VALUE
				|| value.getMonths() == Integer.MIN_VALUE && value.getDays() == Integer.MIN_VALUE && micros == Long.MIN_VALUE;
		for (String text : List.of(postgres(value), verbose(value), sqlStandard(value), value.toString())) {
			if (reserved) assertThrows(DateTimeException.class, () -> SqlIntervalText.parsePostgres(text), text);
			else assertEquals(value, SqlIntervalText.parsePostgres(text), text);
		}
		if (reserved) assertThrows(DateTimeException.class, () -> SqlIntervalText.formatPostgres(value));
		else {
			String input = SqlIntervalText.formatPostgres(value);
			assertEquals(value, SqlIntervalText.parsePostgres(input), input);
		}
		assertEquals(value, SqlIntervalText.parseDuckDb(words(value, true)), words(value, true));
	}

	private static String postgres(SqlInterval value) { return words(value, false); }

	private static String words(SqlInterval value, boolean duck) {
		List<String> parts = new ArrayList<>();
		long month = value.getMonths();
		if (month / 12 != 0) parts.add(month / 12 + " years");
		if (month % 12 != 0) parts.add(month % 12 + (duck ? " months" : " mons"));
		if (value.getDays() != 0) parts.add(value.getDays() + " days");
		parts.add(clock(BigInteger.valueOf(SqlIntervalText.toMicroseconds(value.getTime()))));
		return String.join(" ", parts);
	}

	private static String verbose(SqlInterval value) {
		BigInteger month = BigInteger.valueOf(value.getMonths()), day = BigInteger.valueOf(value.getDays());
		BigInteger micro = BigInteger.valueOf(SqlIntervalText.toMicroseconds(value.getTime()));
		int firstSign = month.signum() != 0 ? month.signum() : day.signum() != 0 ? day.signum() : micro.signum();
		boolean ago = firstSign < 0;
		if (ago) { month = month.negate(); day = day.negate(); micro = micro.negate(); }
		List<String> parts = new ArrayList<>();
		BigInteger[] monthParts = month.divideAndRemainder(BigInteger.valueOf(12));
		add(parts, monthParts[0], "years"); add(parts, monthParts[1], "mons"); add(parts, day, "days");
		BigInteger[] hours = micro.divideAndRemainder(HOUR), minutes = hours[1].divideAndRemainder(MINUTE);
		add(parts, hours[0], "hours"); add(parts, minutes[0], "mins");
		if (minutes[1].signum() != 0) parts.add(decimalSeconds(minutes[1]) + " secs");
		return "@ " + (parts.isEmpty() ? "0" : String.join(" ", parts)) + (ago ? " ago" : "");
	}

	private static void add(List<String> parts, BigInteger value, String unit) {
		if (value.signum() != 0) parts.add(value + " " + unit);
	}

	private static String sqlStandard(SqlInterval value) {
		long month = value.getMonths(), day = value.getDays();
		BigInteger micro = BigInteger.valueOf(SqlIntervalText.toMicroseconds(value.getTime()));
		return (month < 0 ? "-" : "+") + Math.abs(month / 12) + "-" + Math.abs(month % 12)
				+ " " + (day < 0 ? "-" : "+") + Math.abs(day)
				+ " " + (micro.signum() < 0 ? "-" : "+") + clock(micro.abs());
	}

	private static String clock(BigInteger micros) {
		String sign = micros.signum() < 0 ? "-" : "";
		BigInteger[] hours = micros.abs().divideAndRemainder(HOUR), minutes = hours[1].divideAndRemainder(MINUTE);
		String seconds = decimalSeconds(minutes[1]);
		if (minutes[1].compareTo(SECOND.multiply(BigInteger.TEN)) < 0) seconds = "0" + seconds;
		return sign + hours[0] + ":" + (minutes[0].intValue() < 10 ? "0" : "") + minutes[0] + ":" + seconds;
	}

	private static String decimalSeconds(BigInteger micros) {
		BigInteger[] parts = micros.abs().divideAndRemainder(SECOND);
		String fraction = parts[1].signum() == 0 ? "" : "." + String.format("%06d", parts[1]).replaceFirst("0+$", "");
		return (micros.signum() < 0 ? "-" : "") + parts[0] + fraction;
	}
}
