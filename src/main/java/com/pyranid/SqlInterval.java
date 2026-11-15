/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import javax.annotation.concurrent.Immutable;
import java.time.DateTimeException;
import java.time.Duration;
import java.time.Period;

import static java.util.Objects.requireNonNull;

/**
 * An immutable SQL interval with independent calendar-month, calendar-day and elapsed-time components.
 * Months and days are never converted into a guessed elapsed length, and elapsed hours are never carried
 * into calendar days. Each component may have its own sign.
 * <p>
 * Equality compares these fields exactly: one calendar day differs from 24 elapsed hours, and one month
 * differs from 30 days. Database interval equality can use different rules. This type has no natural ordering
 * and does not implement {@code TemporalAmount}: applying calendar fields to a zoned timestamp requires an
 * explicit application policy, which can differ from the database's daylight-saving resolution.
 * <p>
 * Binding and reading depend on the configured database's interval families, precision and range. Column
 * qualifiers, rounding and other SQL coercions can change the stored fields.
 * <p>
 * Oracle and generic drivers need target-family metadata to bind {@link #ZERO}, because the value does not
 * remember which factory created it. Computed values such as {@code SqlInterval.of(Duration.between(a, a))}
 * are also {@code ZERO}. Oracle supplies that metadata only when the parameter is assigned or compared directly
 * to an interval column. In expressions, {@code MERGE}, PL/SQL blocks and {@code INSERT ... SELECT}, binding
 * {@code ZERO} fails even though nonzero values bind. A {@link CustomParameterBinder} that handles only
 * {@code ZERO} and returns {@code fallback()} otherwise can choose the family explicitly.
 *
 * @since 4.9.0
 */
@Immutable
public final class SqlInterval {
	/** The zero interval, without a YEAR-MONTH or DAY-SECOND family preference. */
	@NonNull
	public static final SqlInterval ZERO = new SqlInterval(0, 0, Duration.ZERO);

	private final int months;
	private final int days;
	@NonNull
	private final Duration time;

	private SqlInterval(int months, int days, @NonNull Duration time) {
		this.months = months;
		this.days = days;
		this.time = time;
	}

	/**
	 * Creates an elapsed-only interval. {@code Duration.ofDays(1)} remains 24 elapsed hours, not a calendar day.
	 *
	 * @param time elapsed time
	 * @return an interval with no calendar components
	 */
	@NonNull
	public static SqlInterval of(@NonNull Duration time) {
		return of(0, 0, time);
	}

	/**
	 * Creates a calendar-only interval, folding years into total months while retaining calendar days.
	 * The period's year/month decomposition is not retained.
	 *
	 * @param period calendar period
	 * @return a calendar interval
	 * @throws DateTimeException if total months do not fit a signed 32-bit integer
	 */
	@NonNull
	public static SqlInterval of(@NonNull Period period) {
		return of(period, Duration.ZERO);
	}

	/**
	 * Creates an interval from calendar and elapsed components without combining days and hours.
	 *
	 * @param period calendar period, with years folded into total months
	 * @param time elapsed time
	 * @return an interval retaining the independent components
	 * @throws DateTimeException if total months do not fit a signed 32-bit integer
	 */
	@NonNull
	public static SqlInterval of(@NonNull Period period, @NonNull Duration time) {
		requireNonNull(period);
		requireNonNull(time);
		try {
			return of(Math.toIntExact(period.toTotalMonths()), period.getDays(), time);
		} catch (ArithmeticException failure) {
			throw new DateTimeException("SqlInterval total months exceed the signed 32-bit range", failure);
		}
	}

	/**
	 * Creates an interval without normalizing its three fields. Precision and database-specific range checks
	 * happen when binding; a value may retain nanoseconds even when a particular database supports only microseconds.
	 *
	 * @param months total calendar months
	 * @param days calendar days
	 * @param time elapsed time
	 * @return an interval retaining all three fields
	 */
	@NonNull
	public static SqlInterval of(@NonNull Integer months, @NonNull Integer days, @NonNull Duration time) {
		requireNonNull(months);
		requireNonNull(days);
		requireNonNull(time);
		return months == 0 && days == 0 && time.isZero() ? ZERO : new SqlInterval(months, days, time);
	}

	/** @return total calendar months, with years already folded in */
	@NonNull
	public Integer getMonths() { return this.months; }

	/** @return calendar days, independent of elapsed hours */
	@NonNull
	public Integer getDays() { return this.days; }

	/** @return elapsed time, independent of calendar months and days */
	@NonNull
	public Duration getTime() { return this.time; }

	/**
	 * Returns the elapsed component only when there are no calendar components. Calendar days are deliberately
	 * not assumed to be 24 hours; use {@link Duration#between(java.time.temporal.Temporal, java.time.temporal.Temporal)}
	 * on original instant-compatible timestamps to calculate elapsed time.
	 *
	 * @return elapsed time
	 * @throws DateTimeException if months or days are nonzero
	 */
	@NonNull
	public Duration toDuration() {
		if (this.months != 0 || this.days != 0)
			throw new DateTimeException("SqlInterval calendar months and days have no fixed elapsed length; "
					+ "use Duration.between(...) on the original instants");
		return this.time;
	}

	/**
	 * Returns a normalized calendar period only when the elapsed component is zero. Normalization splits total
	 * months into years and months, retaining the day component unchanged.
	 *
	 * @return calendar period
	 * @throws DateTimeException if elapsed time is nonzero
	 */
	@NonNull
	public Period toPeriod() {
		if (!this.time.isZero())
			throw new DateTimeException("SqlInterval elapsed time cannot be converted to a calendar Period");
		return Period.of(0, this.months, this.days).normalized();
	}

	@Override
	public boolean equals(@Nullable Object other) {
		return this == other || other instanceof SqlInterval interval
				&& this.months == interval.months && this.days == interval.days && this.time.equals(interval.time);
	}

	@Override
	public int hashCode() {
		return 31 * (31 * Integer.hashCode(this.months) + Integer.hashCode(this.days)) + this.time.hashCode();
	}

	/**
	 * Returns an ISO-style field representation with independent component signs, for example
	 * {@code P14M-3DT4H5M6.789S}. This is a value display, not portable SQL literal syntax. No public parser
	 * or text-storage convention is implied.
	 *
	 * @return a representation retaining all components
	 */
	@Override
	@NonNull
	public String toString() {
		if (this.months == 0 && this.days == 0) return this.time.toString();
		StringBuilder result = new StringBuilder("P");
		if (this.months != 0) result.append(this.months).append('M');
		if (this.days != 0) result.append(this.days).append('D');
		if (!this.time.isZero()) {
			String renderedTime = this.time.toString();
			result.append(renderedTime, 1, renderedTime.length());
		}
		return result.toString();
	}
}
