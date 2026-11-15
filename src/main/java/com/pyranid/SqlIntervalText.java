/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.DateTimeException;
import java.time.Duration;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;

/** Exact codecs for database interval output, deliberately not a public/general ISO interval parser. */
final class SqlIntervalText {
	private static final BigInteger MICROS_PER_SECOND = BigInteger.valueOf(1_000_000);
	private static final BigInteger MICROS_PER_MINUTE = BigInteger.valueOf(60_000_000);
	private static final BigInteger MICROS_PER_HOUR = BigInteger.valueOf(3_600_000_000L);
	private static final BigInteger TWELVE = BigInteger.valueOf(12);
	private static final Pattern INTEGER = Pattern.compile("[+-]?[0-9]+");
	private static final Pattern SECONDS = Pattern.compile("[+-]?[0-9]+(?:\\.[0-9]{1,6})?");
	private static final Pattern CLOCK = Pattern.compile("([+-]?)([0-9]+):([0-9]{2}):([0-9]{2})(?:\\.([0-9]{1,6}))?");
	private static final Pattern YEAR_MONTH = Pattern.compile("([+-]?)([0-9]+)-([0-9]{1,2})");
	private static final Pattern ISO = Pattern.compile("P(?:([+-]?[0-9]+)Y)?(?:([+-]?[0-9]+)M)?"
			+ "(?:([+-]?[0-9]+)D)?(?:T(?:([+-]?[0-9]+)H)?(?:([+-]?[0-9]+)M)?"
			+ "(?:([+-]?[0-9]+(?:\\.[0-9]{1,6})?)S)?)?");

	private SqlIntervalText() {}

	static SqlInterval parsePostgres(String text) {
		text = checkedText(text);
		try {
			SqlInterval result;
			if (text.startsWith("P")) result = parseIso(text);
			else if (text.startsWith("@")) result = parseWords(text, false, true);
			else if (containsLetter(text)) result = parseWords(text, false, false);
			else result = parseSqlStandard(text);
			rejectPostgresInfinity(result);
			return result;
		} catch (ArithmeticException | IllegalArgumentException failure) {
			throw invalid("PostgreSQL", failure);
		}
	}

	static SqlInterval parseDuckDb(String text) {
		text = checkedText(text);
		try {
			return parseWords(text, true, false);
		} catch (ArithmeticException | IllegalArgumentException failure) {
			throw invalid("DuckDB", failure);
		}
	}

	static long toMicroseconds(Duration time) {
		requireNonNull(time);
		if (time.getNano() % 1_000 != 0)
			throw new DateTimeException("SqlInterval time has sub-microsecond precision; truncate or round it explicitly "
					+ "before binding to a microsecond-precision database");
		try {
			// A valid Long.MIN_VALUE microsecond duration has floor seconds whose multiplication alone
			// underflows. Add its positive nano fraction in arbitrary precision before checking the result.
			return BigInteger.valueOf(time.getSeconds()).multiply(MICROS_PER_SECOND)
					.add(BigInteger.valueOf(time.getNano() / 1_000)).longValueExact();
		} catch (ArithmeticException failure) {
			throw new DateTimeException("SqlInterval time exceeds the signed 64-bit microsecond range", failure);
		}
	}

	static Duration fromMicroseconds(long microseconds) {
		return Duration.ofSeconds(Math.floorDiv(microseconds, 1_000_000L),
				Math.floorMod(microseconds, 1_000_000L) * 1_000L);
	}

	static void rejectPostgresInfinity(SqlInterval interval) {
		requireNonNull(interval);
		long microseconds = toMicroseconds(interval.getTime());
		if (interval.getMonths() == Integer.MAX_VALUE && interval.getDays() == Integer.MAX_VALUE && microseconds == Long.MAX_VALUE
				|| interval.getMonths() == Integer.MIN_VALUE && interval.getDays() == Integer.MIN_VALUE && microseconds == Long.MIN_VALUE)
			throw new DateTimeException("PostgreSQL reserves this SqlInterval field combination for infinity, which is unsupported");
	}

	static String formatPostgres(SqlInterval interval) {
		rejectPostgresInfinity(interval);
		// Keep the PostgreSQL input encoding independent of SqlInterval's public display format.
		Duration time = interval.getTime();
		if (interval.getMonths() == 0 && interval.getDays() == 0) return time.toString();
		StringBuilder result = new StringBuilder("P");
		if (interval.getMonths() != 0) result.append(interval.getMonths()).append('M');
		if (interval.getDays() != 0) result.append(interval.getDays()).append('D');
		if (!time.isZero()) {
			String renderedTime = time.toString();
			result.append(renderedTime, 1, renderedTime.length());
		}
		return result.toString();
	}

	static String formatDuckDb(SqlInterval interval) {
		requireNonNull(interval);
		long microseconds = toMicroseconds(interval.getTime());
		// DuckDB parses a signed literal's magnitude before applying its sign; spelling Long.MIN_VALUE
		// as one microsecond count overflows that magnitude. Keep both terms in range, without carrying
		// elapsed time into the independent calendar-day field or passing through floating point.
		return interval.getMonths() + " months " + interval.getDays() + " days "
				+ microseconds / 1_000_000L + " seconds " + microseconds % 1_000_000L + " microseconds";
	}

	private static String checkedText(String text) {
		requireNonNull(text);
		text = text.trim();
		// All supported server output forms fit comfortably within this bound, even at signed extrema.
		if (text.isEmpty() || text.length() > 512 || text.equals("infinity") || text.equals("-infinity"))
			throw new DateTimeException("Cannot read an empty, infinite or oversized SQL interval representation");
		return text;
	}

	private static boolean containsLetter(String text) {
		for (int index = 0; index < text.length(); ++index)
			if (Character.isLetter(text.charAt(index))) return true;
		return false;
	}

	private static SqlInterval parseIso(String text) {
		Matcher match = ISO.matcher(text);
		if (!match.matches()) throw new IllegalArgumentException("Invalid ISO interval grammar");
		boolean hasDate = match.group(1) != null || match.group(2) != null || match.group(3) != null;
		boolean hasTime = match.group(4) != null || match.group(5) != null || match.group(6) != null;
		if (!hasDate && !hasTime || text.indexOf('T') >= 0 && !hasTime)
			throw new IllegalArgumentException("Empty ISO interval fields");
		BigInteger months = integerOrZero(match.group(1)).multiply(TWELVE).add(integerOrZero(match.group(2)));
		BigInteger days = integerOrZero(match.group(3));
		BigInteger hours = integerOrZero(match.group(4));
		BigInteger minutes = integerOrZero(match.group(5));
		BigInteger seconds = secondsOrZero(match.group(6));
		checkWithinMinute(minutes, seconds);
		BigInteger micros = hours.multiply(MICROS_PER_HOUR).add(minutes.multiply(MICROS_PER_MINUTE)).add(seconds);
		return interval(months, days, micros);
	}

	private static SqlInterval parseWords(String text, boolean duck, boolean verbose) {
		boolean ago = false;
		if (verbose) {
			if (!text.startsWith("@ ")) throw new IllegalArgumentException("Invalid verbose interval prefix");
			text = text.substring(2).trim();
			if (text.endsWith(" ago")) {
				ago = true;
				text = text.substring(0, text.length() - 4);
			}
			if (text.equals("0")) return SqlInterval.ZERO;
		}
		String[] tokens = text.split("\\s+");
		BigInteger months = BigInteger.ZERO, days = BigInteger.ZERO, micros = BigInteger.ZERO;
		int previousRank = -1;
		for (int index = 0; index < tokens.length;) {
			String number = tokens[index++];
			if (number.indexOf(':') >= 0) {
				if (verbose || previousRank >= 3 || index != tokens.length)
					throw new IllegalArgumentException("Misplaced interval clock");
				micros = clock(number);
				previousRank = 3;
				continue;
			}
			if (index == tokens.length) throw new IllegalArgumentException("Missing interval unit");
			String unit = tokens[index++];
			int rank = rank(unit, duck, verbose);
			if (rank <= previousRank) throw new IllegalArgumentException("Repeated or unordered interval unit");
			previousRank = rank;
			if (rank == 5) {
				BigInteger seconds = seconds(number);
				checkWithinMinute(BigInteger.ZERO, seconds);
				micros = micros.add(seconds);
			} else {
				BigInteger value = integer(number);
				switch (rank) {
					case 0 -> months = months.add(value.multiply(TWELVE));
					case 1 -> months = months.add(value);
					case 2 -> days = value;
					case 3 -> micros = micros.add(value.multiply(MICROS_PER_HOUR));
					case 4 -> {
						checkWithinMinute(value, BigInteger.ZERO);
						micros = micros.add(value.multiply(MICROS_PER_MINUTE));
					}
					default -> throw new IllegalArgumentException("Invalid interval unit");
				}
			}
		}
		if (ago) { months = months.negate(); days = days.negate(); micros = micros.negate(); }
		return interval(months, days, micros);
	}

	private static int rank(String unit, boolean duck, boolean verbose) {
		return switch (unit) {
			case "year", "years" -> 0;
			case "mon", "mons" -> { if (duck) throw new IllegalArgumentException("Invalid DuckDB month unit"); yield 1; }
			case "month", "months" -> { if (!duck) throw new IllegalArgumentException("Invalid PostgreSQL month unit"); yield 1; }
			case "day", "days" -> 2;
			case "hour", "hours" -> { if (!verbose) throw new IllegalArgumentException("Invalid interval hour unit"); yield 3; }
			case "min", "mins" -> { if (!verbose) throw new IllegalArgumentException("Invalid interval minute unit"); yield 4; }
			case "sec", "secs" -> { if (!verbose) throw new IllegalArgumentException("Invalid interval second unit"); yield 5; }
			default -> throw new IllegalArgumentException("Invalid interval unit");
		};
	}

	private static SqlInterval parseSqlStandard(String text) {
		if (text.equals("0")) return SqlInterval.ZERO;
		String[] tokens = text.split("\\s+");
		if (tokens.length == 3) {
			if (!explicitSign(tokens[0]) || !explicitSign(tokens[1]) || !explicitSign(tokens[2]))
				throw new IllegalArgumentException("Mixed SQL interval groups require explicit signs");
			return interval(yearMonth(tokens[0]), integer(tokens[1]), clock(tokens[2]));
		}
		if (tokens.length == 2) {
			if (explicitSign(tokens[1])) throw new IllegalArgumentException("Mixed SQL interval signs require three groups");
			BigInteger days = integer(tokens[0]);
			BigInteger micros = clock(tokens[1]);
			if (tokens[0].startsWith("-")) micros = micros.negate();
			return interval(BigInteger.ZERO, days, micros);
		}
		if (tokens.length == 1) {
			if (text.indexOf(':') >= 0) return interval(BigInteger.ZERO, BigInteger.ZERO, clock(text));
			return interval(yearMonth(text), BigInteger.ZERO, BigInteger.ZERO);
		}
		throw new IllegalArgumentException("Invalid SQL interval group count");
	}

	private static BigInteger yearMonth(String text) {
		Matcher match = YEAR_MONTH.matcher(text);
		if (!match.matches()) throw new IllegalArgumentException("Invalid year-month interval");
		BigInteger years = new BigInteger(match.group(2));
		int months = Integer.parseInt(match.group(3));
		if (months >= 12) throw new IllegalArgumentException("Invalid year-month remainder");
		BigInteger result = years.multiply(TWELVE).add(BigInteger.valueOf(months));
		return match.group(1).equals("-") ? result.negate() : result;
	}

	private static BigInteger clock(String text) {
		Matcher match = CLOCK.matcher(text);
		if (!match.matches()) throw new IllegalArgumentException("Invalid interval clock");
		BigInteger hours = new BigInteger(match.group(2));
		BigInteger minutes = new BigInteger(match.group(3));
		BigInteger seconds = seconds(match.group(4) + (match.group(5) == null ? "" : "." + match.group(5)));
		checkWithinMinute(minutes, seconds);
		BigInteger result = hours.multiply(MICROS_PER_HOUR).add(minutes.multiply(MICROS_PER_MINUTE)).add(seconds);
		return match.group(1).equals("-") ? result.negate() : result;
	}

	private static boolean explicitSign(String text) {
		return text.startsWith("+") || text.startsWith("-");
	}

	private static void checkWithinMinute(BigInteger minutes, BigInteger secondsMicros) {
		if (minutes.abs().compareTo(BigInteger.valueOf(60)) >= 0
				|| secondsMicros.abs().compareTo(MICROS_PER_MINUTE) >= 0)
			throw new IllegalArgumentException("Invalid minute or second remainder");
	}

	private static BigInteger integer(String text) {
		if (!INTEGER.matcher(text).matches()) throw new IllegalArgumentException("Invalid integer interval field");
		return new BigInteger(text);
	}

	private static BigInteger integerOrZero(String text) {
		return text == null ? BigInteger.ZERO : integer(text);
	}

	private static BigInteger seconds(String text) {
		if (!SECONDS.matcher(text).matches()) throw new IllegalArgumentException("Invalid fractional interval field");
		return new BigDecimal(text).movePointRight(6).toBigIntegerExact();
	}

	private static BigInteger secondsOrZero(String text) {
		return text == null ? BigInteger.ZERO : seconds(text);
	}

	private static SqlInterval interval(BigInteger months, BigInteger days, BigInteger microseconds) {
		return SqlInterval.of(months.intValueExact(), days.intValueExact(), fromMicroseconds(microseconds.longValueExact()));
	}

	private static DateTimeException invalid(String vendor, RuntimeException cause) {
		// Avoid echoing row values into diagnostic text, including malformed and overlong inputs.
		return new DateTimeException("Cannot read an exact " + vendor + " interval: malformed or out-of-range fields", cause);
	}
}
