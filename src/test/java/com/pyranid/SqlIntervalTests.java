/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.junit.jupiter.api.Test;

import java.time.DateTimeException;
import java.time.Duration;
import java.time.Period;
import java.time.temporal.TemporalAmount;

import static org.junit.jupiter.api.Assertions.*;

class SqlIntervalTests {
	@Test
	void elapsedAndCalendarDaysRemainDifferentValuesAndHaveDifferentConversions() {
		SqlInterval elapsed = SqlInterval.of(Duration.ofDays(3));
		SqlInterval calendar = SqlInterval.of(Period.ofDays(3));
		assertNotEquals(elapsed, calendar);
		assertEquals(0, elapsed.getMonths());
		assertEquals(0, elapsed.getDays());
		assertEquals(Duration.ofHours(72), elapsed.toDuration());
		assertEquals(3, calendar.getDays());
		assertEquals(Duration.ZERO, calendar.getTime());
		assertEquals(Period.ofDays(3), calendar.toPeriod());
		assertThrows(DateTimeException.class, calendar::toDuration);
		assertThrows(DateTimeException.class, elapsed::toPeriod);
	}

	@Test
	void mixedSignsAreRetainedAndNeitherConversionGuessesMissingContext() {
		Duration time = Duration.ofSeconds(-1, 123456789);
		SqlInterval value = SqlInterval.of(Period.of(1, 2, -3), time);
		assertEquals(14, value.getMonths());
		assertEquals(-3, value.getDays());
		assertSame(time, value.getTime());
		assertThrows(DateTimeException.class, value::toDuration);
		assertThrows(DateTimeException.class, value::toPeriod);
		assertEquals("P14M-3DT-0.876543211S", value.toString());
	}

	@Test
	void zeroIsValueOnlyRegardlessOfItsFactory() {
		for (SqlInterval zero : new SqlInterval[]{SqlInterval.of(Duration.ZERO), SqlInterval.of(Period.ZERO),
				SqlInterval.of(Period.ZERO, Duration.ZERO), SqlInterval.of(0, 0, Duration.ZERO)}) {
			assertSame(SqlInterval.ZERO, zero);
			assertEquals(Duration.ZERO, zero.toDuration());
			assertEquals(Period.ZERO, zero.toPeriod());
			assertEquals("PT0S", zero.toString());
		}
	}

	@Test
	void yearMonthDecompositionIsFoldedAndExplicitPeriodConversionNormalizesIt() {
		SqlInterval years = SqlInterval.of(Period.of(1, 2, 3));
		SqlInterval months = SqlInterval.of(14, 3, Duration.ZERO);
		assertEquals(years, months);
		assertEquals(years.hashCode(), months.hashCode());
		assertEquals(Period.of(1, 2, 3), months.toPeriod());
		assertEquals(Period.of(-1, -2, 3), SqlInterval.of(-14, 3, Duration.ZERO).toPeriod());
		assertEquals(Period.ofMonths(1), SqlInterval.of(Period.of(-1, 13, 0)).toPeriod());
		assertEquals("P14M3D", months.toString());
	}

	@Test
	void factoriesAcceptExactSignedMonthDayLimitsAndRejectUnrepresentableTotalMonths() {
		SqlInterval max = SqlInterval.of(Period.of(178956970, 7, Integer.MAX_VALUE));
		SqlInterval min = SqlInterval.of(Period.of(-178956970, -8, Integer.MIN_VALUE));
		assertEquals(Integer.MAX_VALUE, max.getMonths());
		assertEquals(Integer.MAX_VALUE, max.getDays());
		assertEquals(Integer.MIN_VALUE, min.getMonths());
		assertEquals(Integer.MIN_VALUE, min.getDays());
		assertEquals(Period.of(178956970, 7, Integer.MAX_VALUE), max.toPeriod());
		assertEquals(Period.of(-178956970, -8, Integer.MIN_VALUE), min.toPeriod());
		for (Period invalid : new Period[]{Period.of(178956970, 8, 0), Period.of(-178956970, -9, 0),
				Period.ofYears(Integer.MAX_VALUE), Period.ofYears(Integer.MIN_VALUE)}) {
			assertThrows(DateTimeException.class, () -> SqlInterval.of(invalid));
			assertThrows(DateTimeException.class, () -> SqlInterval.of(invalid, Duration.ofNanos(1)));
		}
	}

	@Test
	void valueRangeAndPrecisionAreNotRestrictedToOneDatabase() {
		for (Duration time : new Duration[]{Duration.ofNanos(1), Duration.ofNanos(-1),
				Duration.ofSeconds(Long.MAX_VALUE, 999999999), Duration.ofSeconds(Long.MIN_VALUE)}) {
			SqlInterval value = SqlInterval.of(time);
			assertEquals(time, value.toDuration());
			assertEquals(time.toString(), value.toString());
		}
	}

	@Test
	void equalityDoesNotUseSqlsGuessedDayOrMonthLengthsAndHasNoOrderingOrTemporalPolicy() {
		SqlInterval month = SqlInterval.of(Period.ofMonths(1));
		assertNotEquals(month, SqlInterval.of(Period.ofDays(30)));
		assertNotEquals(month, SqlInterval.of(Duration.ofDays(30)));
		assertNotEquals(month, null);
		assertNotEquals(month, month.toString());
		assertEquals(month, month);
		assertFalse(Comparable.class.isAssignableFrom(SqlInterval.class));
		assertFalse(TemporalAmount.class.isAssignableFrom(SqlInterval.class));
	}

	@Test
	void factoryArgumentsAreNonNull() {
		assertThrows(NullPointerException.class, () -> SqlInterval.of((Duration) null));
		assertThrows(NullPointerException.class, () -> SqlInterval.of((Period) null));
		assertThrows(NullPointerException.class, () -> SqlInterval.of(null, Duration.ZERO));
		assertThrows(NullPointerException.class, () -> SqlInterval.of(Period.ZERO, null));
		assertThrows(NullPointerException.class, () -> SqlInterval.of(null, 0, Duration.ZERO));
		assertThrows(NullPointerException.class, () -> SqlInterval.of(0, null, Duration.ZERO));
		assertThrows(NullPointerException.class, () -> SqlInterval.of(0, 0, null));
	}
}
