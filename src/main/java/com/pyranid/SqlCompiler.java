/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pyranid;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/** Compiles dialect-sensitive named SQL; cached values contain no JDBC or row state. */
final class SqlCompiler {
	private final Map<String, ParsedSqlVariants> cache;

	SqlCompiler(int capacity) {
		if (capacity < 0)
			throw new IllegalArgumentException("parsedSqlCacheCapacity must be >= 0");
		this.cache = capacity == 0 ? null : new ConcurrentLruMap<>(capacity);
	}

	@NonNull
	ParsedSqlVariants compile(@NonNull String sql) {
		requireNonNull(sql);

		if (this.cache == null)
			return parseNamedParameterSqlVariants(sql);

		return this.cache.computeIfAbsent(sql, SqlCompiler::parseNamedParameterSqlVariants);
	}

	@NonNull
	private static ParsedSqlVariants parseNamedParameterSqlVariants(@NonNull String sql) {
		requireNonNull(sql);

		ParsedSql standardParsedSql = null;
		ParsedSql mysqlParsedSql = null;
		ParsedSql duckDbParsedSql = null;
		ParsedSql bracketIdentifierParsedSql = null;
		IllegalArgumentException standardParseFailure = null;
		IllegalArgumentException mysqlParseFailure = null;
		IllegalArgumentException duckDbParseFailure = null;
		IllegalArgumentException bracketIdentifierParseFailure = null;

		try {
			standardParsedSql = parseNamedParameterSql(sql, SqlLexicalMode.STANDARD);
		} catch (IllegalArgumentException e) {
			standardParseFailure = e;
		}

		if (requiresMySqlLexicalVariant(sql)) {
			try {
				mysqlParsedSql = parseNamedParameterSql(sql, SqlLexicalMode.MYSQL);
			} catch (IllegalArgumentException e) {
				mysqlParseFailure = e;
			}
		} else {
			mysqlParsedSql = standardParsedSql;
			mysqlParseFailure = standardParseFailure;
		}

		if (requiresDuckDbLexicalVariant(sql)) {
			try {
				duckDbParsedSql = parseNamedParameterSql(sql, SqlLexicalMode.DUCK_DB);
			} catch (IllegalArgumentException e) {
				duckDbParseFailure = e;
			}
		} else {
			duckDbParsedSql = standardParsedSql;
			duckDbParseFailure = standardParseFailure;
		}

		if (requiresDuckDbLexicalVariant(sql)) {
			try {
				bracketIdentifierParsedSql = parseNamedParameterSql(sql, SqlLexicalMode.BRACKET_IDENTIFIERS);
			} catch (IllegalArgumentException e) {
				bracketIdentifierParseFailure = e;
			}
		} else {
			bracketIdentifierParsedSql = standardParsedSql;
			bracketIdentifierParseFailure = standardParseFailure;
		}

		if (standardParsedSql == null && mysqlParsedSql == null && duckDbParsedSql == null && bracketIdentifierParsedSql == null)
			throw requireNonNull(standardParseFailure != null ? standardParseFailure
					: mysqlParseFailure != null ? mysqlParseFailure : duckDbParseFailure);

		ParsedSql fallbackParsedSql = standardParsedSql != null ? standardParsedSql
				: mysqlParsedSql != null ? mysqlParsedSql
				: duckDbParsedSql != null ? duckDbParsedSql : requireNonNull(bracketIdentifierParsedSql);

		ParsedSql effectiveStandardParsedSql = standardParsedSql == null
				? fallbackParsedSql
				: standardParsedSql;
		ParsedSql effectiveMysqlParsedSql = mysqlParsedSql == null
				? fallbackParsedSql
				: mysqlParsedSql;
		ParsedSql effectiveDuckDbParsedSql = duckDbParsedSql == null
				? fallbackParsedSql
				: duckDbParsedSql;
		ParsedSql effectiveBracketIdentifierParsedSql = bracketIdentifierParsedSql == null
				? fallbackParsedSql
				: bracketIdentifierParsedSql;
		Set<String> combinedParameterNames = new HashSet<>(effectiveStandardParsedSql.distinctParameterNames);
		combinedParameterNames.addAll(effectiveMysqlParsedSql.distinctParameterNames);
		combinedParameterNames.addAll(effectiveDuckDbParsedSql.distinctParameterNames);
		combinedParameterNames.addAll(effectiveBracketIdentifierParsedSql.distinctParameterNames);
		Set<String> distinctParameterNames = Set.copyOf(combinedParameterNames);

		boolean requiresDatabaseType = standardParseFailure != null
				|| mysqlParseFailure != null
				|| duckDbParseFailure != null
				|| bracketIdentifierParseFailure != null
				|| !effectiveStandardParsedSql.equivalentTo(effectiveMysqlParsedSql)
				|| !effectiveStandardParsedSql.equivalentTo(effectiveDuckDbParsedSql)
				|| !effectiveStandardParsedSql.equivalentTo(effectiveBracketIdentifierParsedSql);

		return new ParsedSqlVariants(effectiveStandardParsedSql,
				effectiveMysqlParsedSql,
				effectiveDuckDbParsedSql,
				effectiveBracketIdentifierParsedSql,
				standardParseFailure == null ? null : requireNonNull(standardParseFailure.getMessage()),
				mysqlParseFailure == null ? null : requireNonNull(mysqlParseFailure.getMessage()),
				duckDbParseFailure == null ? null : requireNonNull(duckDbParseFailure.getMessage()),
				bracketIdentifierParseFailure == null ? null : requireNonNull(bracketIdentifierParseFailure.getMessage()),
				distinctParameterNames,
				requiresDatabaseType);
	}

	private static boolean requiresMySqlLexicalVariant(@NonNull String sql) {
		requireNonNull(sql);
		return sql.indexOf('#') >= 0 || sql.indexOf("\\'") >= 0;
	}

	private static boolean requiresDuckDbLexicalVariant(@NonNull String sql) {
		requireNonNull(sql);
		return sql.indexOf('[') >= 0;
	}

	static final class ParsedSqlVariants {
		@NonNull
		final ParsedSql standardParsedSql;
		@NonNull
		final ParsedSql mysqlParsedSql;
		@NonNull
		final ParsedSql duckDbParsedSql;
		@NonNull
		final ParsedSql bracketIdentifierParsedSql;
		@Nullable
		final String standardParseFailureMessage;
		@Nullable
		final String mysqlParseFailureMessage;
		@Nullable
		final String duckDbParseFailureMessage;
		@Nullable
		final String bracketIdentifierParseFailureMessage;
		@NonNull
		final Set<String> distinctParameterNames;
		final boolean requiresDatabaseType;

		private ParsedSqlVariants(@NonNull ParsedSql standardParsedSql,
														@NonNull ParsedSql mysqlParsedSql,
														@NonNull ParsedSql duckDbParsedSql,
														@NonNull ParsedSql bracketIdentifierParsedSql,
														@Nullable String standardParseFailureMessage,
														@Nullable String mysqlParseFailureMessage,
														@Nullable String duckDbParseFailureMessage,
														@Nullable String bracketIdentifierParseFailureMessage,
														@NonNull Set<String> distinctParameterNames,
														boolean requiresDatabaseType) {
			this.standardParsedSql = requireNonNull(standardParsedSql);
			this.mysqlParsedSql = requireNonNull(mysqlParsedSql);
			this.duckDbParsedSql = requireNonNull(duckDbParsedSql);
			this.bracketIdentifierParsedSql = requireNonNull(bracketIdentifierParsedSql);
			this.standardParseFailureMessage = standardParseFailureMessage;
			this.mysqlParseFailureMessage = mysqlParseFailureMessage;
			this.duckDbParseFailureMessage = duckDbParseFailureMessage;
			this.bracketIdentifierParseFailureMessage = bracketIdentifierParseFailureMessage;
			this.distinctParameterNames = Set.copyOf(requireNonNull(distinctParameterNames));
			this.requiresDatabaseType = requiresDatabaseType;
		}
	}

	static final class ParsedSql {
		@NonNull
		final List<String> sqlFragments;
		@NonNull
		final List<@NonNull List<@NonNull Integer>> questionMarkOperatorFragmentIndexes;
		final boolean hasQuestionMarkOperators;
		@NonNull
		final List<String> parameterNames;
		@NonNull
		final Set<String> distinctParameterNames;

		private ParsedSql(@NonNull List<String> sqlFragments,
											@NonNull List<@NonNull List<@NonNull Integer>> questionMarkOperatorFragmentIndexes,
											@NonNull List<String> parameterNames,
											@NonNull Set<String> distinctParameterNames) {
			requireNonNull(sqlFragments);
			requireNonNull(questionMarkOperatorFragmentIndexes);
			requireNonNull(parameterNames);
			requireNonNull(distinctParameterNames);

			this.sqlFragments = sqlFragments;
			this.questionMarkOperatorFragmentIndexes = questionMarkOperatorFragmentIndexes.stream()
					.map(List::copyOf)
					.toList();
			this.hasQuestionMarkOperators = questionMarkOperatorFragmentIndexes.stream().anyMatch(indexes -> !indexes.isEmpty());
			this.parameterNames = parameterNames;
			this.distinctParameterNames = distinctParameterNames;
		}

		private boolean equivalentTo(@NonNull ParsedSql other) {
			requireNonNull(other);
			return this.sqlFragments.equals(other.sqlFragments)
					&& this.questionMarkOperatorFragmentIndexes.equals(other.questionMarkOperatorFragmentIndexes)
					&& this.parameterNames.equals(other.parameterNames);
		}
	}

	enum SqlLexicalMode {
		STANDARD,
		MYSQL,
		DUCK_DB,
		BRACKET_IDENTIFIERS
	}

	@NonNull
	static ParsedSql parseNamedParameterSql(@NonNull String sql) {
		return parseNamedParameterSql(sql, SqlLexicalMode.STANDARD);
	}

	@NonNull
	static ParsedSql parseNamedParameterSql(@NonNull String sql,
																					@NonNull SqlLexicalMode lexicalMode) {
		requireNonNull(sql);
		requireNonNull(lexicalMode);

		List<String> sqlFragments = new ArrayList<>();
		StringBuilder sqlFragment = new StringBuilder(sql.length());
		List<List<Integer>> questionMarkOperatorFragmentIndexes = new ArrayList<>();
		List<Integer> currentQuestionMarkOperatorIndexes = new ArrayList<>();
		List<String> parameterNames = new ArrayList<>();
		Set<String> distinctParameterNames = new HashSet<>();

		boolean inSingleQuote = false;
		boolean inSingleQuoteEscapesBackslash = false;
		int singleQuoteStartIndex = -1;
		boolean inDoubleQuote = false;
		int doubleQuoteStartIndex = -1;
		boolean inBacktickQuote = false;
		int backtickQuoteStartIndex = -1;
		boolean inBracketQuote = false;
		int bracketQuoteStartIndex = -1;
		boolean inLineComment = false;
		int blockCommentDepth = 0;
		int blockCommentStartIndex = -1;
		String dollarQuoteDelimiter = null;
		int dollarQuoteStartIndex = -1;
		Character oracleQuoteClosingDelimiter = null;
		int oracleQuoteStartIndex = -1;
		int previousMeaningfulIndex = -1;

		for (int i = 0; i < sql.length(); ) {
			if (oracleQuoteClosingDelimiter != null) {
				char c = sql.charAt(i);
				sqlFragment.append(c);

				if (c == oracleQuoteClosingDelimiter && i + 1 < sql.length() && sql.charAt(i + 1) == '\'') {
					sqlFragment.append('\'');
					i += 2;
					oracleQuoteClosingDelimiter = null;
					oracleQuoteStartIndex = -1;
					previousMeaningfulIndex = i - 1;
				} else {
					++i;
				}

				continue;
			}

			if (dollarQuoteDelimiter != null) {
				if (sql.startsWith(dollarQuoteDelimiter, i)) {
					sqlFragment.append(dollarQuoteDelimiter);
					previousMeaningfulIndex = i + dollarQuoteDelimiter.length() - 1;
					i += dollarQuoteDelimiter.length();
					dollarQuoteDelimiter = null;
					dollarQuoteStartIndex = -1;
				} else {
					sqlFragment.append(sql.charAt(i));
					++i;
				}

				continue;
			}

			char c = sql.charAt(i);

			if (inLineComment) {
				sqlFragment.append(c);
				++i;

				if (c == '\n' || c == '\r')
					inLineComment = false;

				continue;
			}

			if (blockCommentDepth > 0) {
				if (c == '/' && i + 1 < sql.length() && sql.charAt(i + 1) == '*') {
					sqlFragment.append("/*");
					i += 2;
					++blockCommentDepth;
				} else if (c == '*' && i + 1 < sql.length() && sql.charAt(i + 1) == '/') {
					sqlFragment.append("*/");
					i += 2;
					--blockCommentDepth;
					if (blockCommentDepth == 0)
						blockCommentStartIndex = -1;
				} else {
					sqlFragment.append(c);
					++i;
				}

				continue;
			}

			if (inSingleQuote) {
				sqlFragment.append(c);

				if (inSingleQuoteEscapesBackslash && c == '\\' && i + 1 < sql.length()) {
					sqlFragment.append(sql.charAt(i + 1));
					i += 2;
					continue;
				}

				if (c == '\'') {
					// Escaped quote: ''
					if (i + 1 < sql.length() && sql.charAt(i + 1) == '\'') {
						sqlFragment.append('\'');
						i += 2;
						continue;
					}

					inSingleQuote = false;
					inSingleQuoteEscapesBackslash = false;
					singleQuoteStartIndex = -1;
					previousMeaningfulIndex = i;
				}

				++i;
				continue;
			}

			if (inDoubleQuote) {
				sqlFragment.append(c);

				if (c == '"') {
					// Escaped quote: ""
					if (i + 1 < sql.length() && sql.charAt(i + 1) == '"') {
						sqlFragment.append('"');
						i += 2;
						continue;
					}

					inDoubleQuote = false;
					doubleQuoteStartIndex = -1;
					previousMeaningfulIndex = i;
				}

				++i;
				continue;
			}

			if (inBacktickQuote) {
				sqlFragment.append(c);

				if (c == '`') {
					inBacktickQuote = false;
					backtickQuoteStartIndex = -1;
					previousMeaningfulIndex = i;
				}

				++i;
				continue;
			}

			if (inBracketQuote) {
				sqlFragment.append(c);

				if (c == ']' && i + 1 < sql.length() && sql.charAt(i + 1) == ']') {
					sqlFragment.append(']');
					i += 2;
					continue;
				}

				if (c == ']') {
					inBracketQuote = false;
					bracketQuoteStartIndex = -1;
					previousMeaningfulIndex = i;
				}

				++i;
				continue;
			}

			// Not inside string/comment
			if (lexicalMode == SqlLexicalMode.MYSQL && c == '#') {
				sqlFragment.append(c);
				++i;
				inLineComment = true;
				continue;
			}

			if (c == '-' && i + 1 < sql.length() && sql.charAt(i + 1) == '-') {
				sqlFragment.append("--");
				i += 2;
				inLineComment = true;
				continue;
			}

			if (c == '/' && i + 1 < sql.length() && sql.charAt(i + 1) == '*') {
				sqlFragment.append("/*");
				i += 2;
				blockCommentDepth = 1;
				blockCommentStartIndex = i - 2;
				continue;
			}

			if ((c == 'U' || c == 'u') && !isIdentifierContinuation(sql, i)
					&& i + 2 < sql.length() && sql.charAt(i + 1) == '&' && sql.charAt(i + 2) == '\'') {
				inSingleQuote = true;
				inSingleQuoteEscapesBackslash = true;
				singleQuoteStartIndex = i;
				sqlFragment.append(c).append("&'");
				i += 3;
				continue;
			}

			if ((c == 'E' || c == 'e') && !isIdentifierContinuation(sql, i)
					&& i + 1 < sql.length() && sql.charAt(i + 1) == '\'') {
				inSingleQuote = true;
				inSingleQuoteEscapesBackslash = true;
				singleQuoteStartIndex = i;
				sqlFragment.append(c).append('\'');
				i += 2;
				continue;
			}

			if ((c == 'Q' || c == 'q') && !isIdentifierContinuation(sql, i)
					&& i + 2 < sql.length() && sql.charAt(i + 1) == '\''
					&& !Character.isWhitespace(sql.charAt(i + 2)) && sql.charAt(i + 2) != '\'') {
				char openingDelimiter = sql.charAt(i + 2);
				oracleQuoteClosingDelimiter = oracleQuoteClosingDelimiter(openingDelimiter);
				oracleQuoteStartIndex = i;
				sqlFragment.append(c).append('\'').append(openingDelimiter);
				i += 3;
				continue;
			}

			if (c == '\'') {
				inSingleQuote = true;
				inSingleQuoteEscapesBackslash = lexicalMode == SqlLexicalMode.MYSQL;
				singleQuoteStartIndex = i;
				sqlFragment.append(c);
				++i;
				continue;
			}

			if (c == '"') {
				inDoubleQuote = true;
				doubleQuoteStartIndex = i;
				sqlFragment.append(c);
				++i;
				continue;
			}

			if (c == '`') {
				inBacktickQuote = true;
				backtickQuoteStartIndex = i;
				sqlFragment.append(c);
				++i;
				continue;
			}

			// SQL Server and SQLite always treat brackets as quoted identifiers, including after
			// expressions (implicit aliases) and WHERE. Other modes retain their subscript behavior.
			if (lexicalMode != SqlLexicalMode.DUCK_DB && c == '['
					&& (lexicalMode == SqlLexicalMode.BRACKET_IDENTIFIERS || !isBracketSubscriptStart(sql, i))) {
				inBracketQuote = true;
				bracketQuoteStartIndex = i;
				sqlFragment.append(c);
				++i;
				continue;
			}

			if (c == '$' && !isIdentifierContinuation(sql, i)) {
				String delimiter = parseDollarQuoteDelimiter(sql, i);

				if (delimiter != null) {
					sqlFragment.append(delimiter);
					i += delimiter.length();
					dollarQuoteDelimiter = delimiter;
					dollarQuoteStartIndex = i - delimiter.length();
					continue;
				}
			}

			if (c == '?') {
				if (isAllowedQuestionMarkOperator(sql, i, previousMeaningfulIndex)) {
					currentQuestionMarkOperatorIndexes.add(sqlFragment.length());
					sqlFragment.append(c);
					++i;
					continue;
				}

				throw new IllegalArgumentException(format("Positional parameters ('?') are not supported. Use named parameters (e.g. ':id') and %s#bind. SQL: %s",
						Query.class.getSimpleName(), sql));
			}

			if (c == ':' && i + 1 < sql.length() && sql.charAt(i + 1) == ':') {
				// Postgres type-cast operator (::), do not treat second ':' as a parameter prefix.
				sqlFragment.append("::");
				i += 2;
				continue;
			}

			if (c == ':' && i + 1 < sql.length() && Character.isJavaIdentifierStart(sql.charAt(i + 1))) {
				int nameStartIndex = i + 1;
				int nameEndIndex = nameStartIndex + 1;

				while (nameEndIndex < sql.length() && Character.isJavaIdentifierPart(sql.charAt(nameEndIndex)))
					++nameEndIndex;

				String parameterName = sql.substring(nameStartIndex, nameEndIndex);
				parameterNames.add(parameterName);
				distinctParameterNames.add(parameterName);
				sqlFragments.add(sqlFragment.toString());
				questionMarkOperatorFragmentIndexes.add(List.copyOf(currentQuestionMarkOperatorIndexes));
				currentQuestionMarkOperatorIndexes.clear();
				sqlFragment.setLength(0);
				i = nameEndIndex;
				previousMeaningfulIndex = nameEndIndex - 1;
				continue;
			}

			sqlFragment.append(c);
			if (!Character.isWhitespace(c))
				previousMeaningfulIndex = i;
			++i;
		}

		validateParserTerminalState(sql, inSingleQuote, singleQuoteStartIndex, inDoubleQuote, doubleQuoteStartIndex,
				inBacktickQuote, backtickQuoteStartIndex, inBracketQuote, bracketQuoteStartIndex,
				blockCommentDepth, blockCommentStartIndex, dollarQuoteDelimiter, dollarQuoteStartIndex,
				oracleQuoteClosingDelimiter, oracleQuoteStartIndex);

		sqlFragments.add(sqlFragment.toString());
		questionMarkOperatorFragmentIndexes.add(List.copyOf(currentQuestionMarkOperatorIndexes));

		return new ParsedSql(List.copyOf(sqlFragments), List.copyOf(questionMarkOperatorFragmentIndexes),
				List.copyOf(parameterNames), Set.copyOf(distinctParameterNames));
	}

	private static char oracleQuoteClosingDelimiter(char openingDelimiter) {
		return switch (openingDelimiter) {
			case '[' -> ']';
			case '{' -> '}';
			case '(' -> ')';
			case '<' -> '>';
			default -> openingDelimiter;
		};
	}

	@Nullable
	private static String parseDollarQuoteDelimiter(@NonNull String sql,
																									int startIndex) {
		requireNonNull(sql);

		if (startIndex < 0 || startIndex >= sql.length())
			return null;

		if (sql.charAt(startIndex) != '$')
			return null;

		int i = startIndex + 1;

		if (i >= sql.length())
			return null;

		char firstTagCharacter = sql.charAt(i);

		if (firstTagCharacter == '$')
			return "$$";

		if (!isDollarQuoteTagStart(firstTagCharacter))
			return null;

		++i;

		while (i < sql.length()) {
			char c = sql.charAt(i);

			if (c == '$')
				return sql.substring(startIndex, i + 1);

			if (!isDollarQuoteTagPart(c))
				return null;

			++i;
		}

		return null;
	}

	private static boolean isDollarQuoteTagStart(char character) {
		return Character.isLetter(character) || character == '_';
	}

	private static boolean isDollarQuoteTagPart(char character) {
		return Character.isLetterOrDigit(character) || character == '_';
	}

	private static boolean isIdentifierContinuation(@NonNull String sql,
																								 int startIndex) {
		requireNonNull(sql);

		if (startIndex <= 0)
			return false;

		return Character.isJavaIdentifierPart(sql.charAt(startIndex - 1));
	}

	private static boolean isBracketSubscriptStart(@NonNull String sql,
																								 int startIndex) {
		requireNonNull(sql);

		if (startIndex <= 0)
			return false;

		int previousIndex = startIndex - 1;

		while (previousIndex >= 0 && Character.isWhitespace(sql.charAt(previousIndex)))
			--previousIndex;

		if (previousIndex < 0)
			return false;

		char previousChar = sql.charAt(previousIndex);
		if (Character.isJavaIdentifierPart(previousChar)) {
			int tokenStartIndex = previousIndex;

			while (tokenStartIndex > 0 && Character.isJavaIdentifierPart(sql.charAt(tokenStartIndex - 1)))
				--tokenStartIndex;

			String previousToken = sql.substring(tokenStartIndex, previousIndex + 1).toUpperCase(Locale.ROOT);
			return !BRACKET_IDENTIFIER_CONTEXT_KEYWORDS.contains(previousToken);
		}

		return previousChar == ')'
				|| previousChar == ']'
				|| previousChar == '"';
	}

	private static void validateParserTerminalState(@NonNull String sql,
																									boolean inSingleQuote,
																									int singleQuoteStartIndex,
																									boolean inDoubleQuote,
																									int doubleQuoteStartIndex,
																									boolean inBacktickQuote,
																									int backtickQuoteStartIndex,
																									boolean inBracketQuote,
																									int bracketQuoteStartIndex,
																									int blockCommentDepth,
																									int blockCommentStartIndex,
																														@Nullable String dollarQuoteDelimiter,
																														int dollarQuoteStartIndex,
																														@Nullable Character oracleQuoteClosingDelimiter,
																														int oracleQuoteStartIndex) {
		requireNonNull(sql);

		if (inSingleQuote)
			throw unterminatedSqlConstructException("single-quoted string", singleQuoteStartIndex, sql);
		if (inDoubleQuote)
			throw unterminatedSqlConstructException("double-quoted identifier", doubleQuoteStartIndex, sql);
		if (inBacktickQuote)
			throw unterminatedSqlConstructException("backtick-quoted identifier", backtickQuoteStartIndex, sql);
		if (inBracketQuote)
			throw unterminatedSqlConstructException("bracket-quoted identifier", bracketQuoteStartIndex, sql);
		if (blockCommentDepth > 0)
			throw unterminatedSqlConstructException("block comment", blockCommentStartIndex, sql);
		if (dollarQuoteDelimiter != null)
			throw unterminatedSqlConstructException(format("dollar-quoted string %s", dollarQuoteDelimiter), dollarQuoteStartIndex, sql);
		if (oracleQuoteClosingDelimiter != null)
			throw unterminatedSqlConstructException("Oracle alternative-quoted string", oracleQuoteStartIndex, sql);
	}

	@NonNull
	private static IllegalArgumentException unterminatedSqlConstructException(@NonNull String construct,
																																			 int startIndex,
																																			 @NonNull String sql) {
		requireNonNull(construct);
		requireNonNull(sql);
		return new IllegalArgumentException(format("Unterminated %s starting at index %s. SQL: %s", construct, startIndex, sql));
	}

	@NonNull
	private static final Set<@NonNull String> QUESTION_MARK_PREFIX_KEYWORDS = Set.of(
			"SELECT", "WHERE", "AND", "OR", "ON", "HAVING", "WHEN", "THEN", "ELSE", "IN",
			"VALUES", "SET", "RETURNING", "USING", "LIKE", "BETWEEN", "IS", "NOT", "NULL",
			"JOIN", "FROM"
	);

	@NonNull
	private static final Set<@NonNull String> BRACKET_IDENTIFIER_CONTEXT_KEYWORDS = Set.of(
			"SELECT", "FROM", "JOIN", "AS", "INTO", "UPDATE", "TABLE", "BY", "ORDER", "GROUP", "SET"
	);

	@NonNull
	private static final Set<@NonNull String> QUESTION_MARK_SUFFIX_KEYWORDS = Set.of(
			"FROM", "WHERE", "AND", "OR", "GROUP", "ORDER", "HAVING", "LIMIT", "OFFSET",
			"UNION", "EXCEPT", "INTERSECT", "RETURNING", "JOIN", "ON"
	);

	private static boolean isAllowedQuestionMarkOperator(@NonNull String sql,
																											 int questionMarkIndex,
																											 int previousMeaningfulIndex) {
		requireNonNull(sql);

		int previousIndex = previousMeaningfulIndex;
		int nextIndex = nextNonWhitespaceIndex(sql, questionMarkIndex + 1);

		if (previousIndex < 0 || nextIndex < 0)
			return false;

		char previousChar = sql.charAt(previousIndex);
		char nextChar = sql.charAt(nextIndex);

		if (isOperatorBeforeQuestionMark(previousChar))
			return false;

		if (isTerminatorAfterQuestionMark(nextChar))
			return false;

		if (questionMarkIndex + 1 < sql.length()) {
			char immediateNextChar = sql.charAt(questionMarkIndex + 1);
			if (immediateNextChar == '|' || immediateNextChar == '&') {
				if (questionMarkIndex + 2 < sql.length() && sql.charAt(questionMarkIndex + 2) == immediateNextChar)
					return false;

				String previousKeyword = keywordBefore(sql, previousIndex);
				if (previousKeyword != null && QUESTION_MARK_PREFIX_KEYWORDS.contains(previousKeyword)
						&& !isNamedParameterKeywordBefore(sql, previousIndex, previousKeyword))
					return false;

				String nextKeyword = keywordAfter(sql, nextIndex);
				if (nextKeyword != null && QUESTION_MARK_SUFFIX_KEYWORDS.contains(nextKeyword))
					return false;

				return true;
			}
		}

		String previousKeyword = keywordBefore(sql, previousIndex);
		if (previousKeyword != null && QUESTION_MARK_PREFIX_KEYWORDS.contains(previousKeyword))
			return false;

		String nextKeyword = keywordAfter(sql, nextIndex);
		if (nextKeyword != null && QUESTION_MARK_SUFFIX_KEYWORDS.contains(nextKeyword))
			return false;

		return true;
	}

	private static boolean isOperatorBeforeQuestionMark(char c) {
		return switch (c) {
			case '=', '<', '>', '!', '+', '-', '*', '/', '%', ',', '(' -> true;
			default -> false;
		};
	}

	private static boolean isTerminatorAfterQuestionMark(char c) {
		return switch (c) {
			case ')', ',', ';' -> true;
			default -> false;
		};
	}

	private static int nextNonWhitespaceIndex(@NonNull String sql,
																						int startIndex) {
		for (int i = startIndex; i < sql.length(); i++)
			if (!Character.isWhitespace(sql.charAt(i)))
				return i;
		return -1;
	}

	@Nullable
	private static String keywordBefore(@NonNull String sql,
																			int index) {
		char c = sql.charAt(index);
		if (!Character.isJavaIdentifierPart(c))
			return null;

		int endIndex = index + 1;
		int startIndex = index;
		while (startIndex >= 0 && Character.isJavaIdentifierPart(sql.charAt(startIndex)))
			--startIndex;

		return sql.substring(startIndex + 1, endIndex).toUpperCase(Locale.ROOT);
	}

	private static boolean isNamedParameterKeywordBefore(@NonNull String sql,
																											 int keywordEndIndex,
																											 @NonNull String keyword) {
		requireNonNull(sql);
		requireNonNull(keyword);

		int startIndex = keywordEndIndex - keyword.length() + 1;
		return startIndex > 0 && sql.charAt(startIndex - 1) == ':';
	}

	@Nullable
	private static String keywordAfter(@NonNull String sql,
																		 int index) {
		char c = sql.charAt(index);
		if (!Character.isJavaIdentifierPart(c))
			return null;

		int endIndex = index + 1;
		while (endIndex < sql.length() && Character.isJavaIdentifierPart(sql.charAt(endIndex)))
			++endIndex;

		return sql.substring(index, endIndex).toUpperCase(Locale.ROOT);
	}

}
