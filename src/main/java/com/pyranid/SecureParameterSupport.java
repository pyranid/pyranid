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

import javax.annotation.concurrent.ThreadSafe;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.function.UnaryOperator;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Internal helpers for secure parameter handling.
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 4.4.0
 */
@ThreadSafe
final class SecureParameterSupport {
	static final int MAX_NESTING_DEPTH = 64;
	@NonNull
	static final String DEFAULT_MASK = "<redacted>";
	/**
	 * Secure values whose rendered form is shorter than this are not scrubbed from diagnostic text -
	 * replacing very short needles (e.g. {@code "1"}, {@code "ab"}) would corrupt unrelated diagnostics
	 * (error codes, SQLStates, positions) for negligible protection.
	 */
	static final int MIN_SCRUB_NEEDLE_LENGTH = 3;

	private SecureParameterSupport() {
		// Prevents instantiation
	}

	/**
	 * A secure value's rendered form paired with the mask that should replace it in diagnostic text.
	 */
	record NeedleMask(@NonNull String needle,
										@NonNull String mask) {}

	/**
	 * The result of computing a diagnostic scrub for a parameter list: a redactor to apply to
	 * Pyranid-rendered diagnostic text (or {@code null} when no secure parameters are present - the
	 * fast path), plus descriptions (class names only, never values) of any failures encountered
	 * while rendering secure values into needles.
	 */
	record DiagnosticScrub(@Nullable UnaryOperator<String> redactor,
												 @NonNull List<String> needleRenderingFailureDescriptions) {
		@NonNull
		static final DiagnosticScrub NONE = new DiagnosticScrub(null, List.of());
	}

	/**
	 * Computes a best-effort diagnostic scrub for the given parameters (a flat parameter list, or a
	 * list of {@link List} parameter groups for batch statements).
	 * <p>
	 * Only values explicitly marked via {@link SecureParameter} contribute needles. A needle is the
	 * {@link String#valueOf(Object)} rendering of the innermost bound value; {@code null} and
	 * {@link Boolean} values are skipped (their renderings - {@code "null"}, {@code "true"},
	 * {@code "false"} - routinely appear in driver text with unrelated meaning), as are renderings
	 * shorter than {@link #MIN_SCRUB_NEEDLE_LENGTH}.
	 */
	@NonNull
	static DiagnosticScrub diagnosticScrubForParameters(@Nullable List<@Nullable Object> parameters) {
		if (parameters == null || parameters.isEmpty())
			return DiagnosticScrub.NONE;

		List<NeedleMask> needleMasks = new ArrayList<>(4);
		List<String> needleRenderingFailureDescriptions = new ArrayList<>(0);

		for (Object parameter : parameters) {
			// Batch statements carry parameters as a list of parameter groups; walk one level of nesting
			if (parameter instanceof List<?> parameterGroup) {
				for (Object groupElement : parameterGroup)
					collectSecureNeedles(groupElement, needleMasks, needleRenderingFailureDescriptions);
			} else {
				collectSecureNeedles(parameter, needleMasks, needleRenderingFailureDescriptions);
			}
		}

		if (needleMasks.isEmpty())
			return needleRenderingFailureDescriptions.isEmpty()
					? DiagnosticScrub.NONE
					: new DiagnosticScrub(null, List.copyOf(needleRenderingFailureDescriptions));

		// Dedupe by needle (first mask wins), then order longest-first so scrubbing is leftmost-longest
		Map<String, String> maskByNeedle = new LinkedHashMap<>(needleMasks.size());

		for (NeedleMask needleMask : needleMasks)
			maskByNeedle.putIfAbsent(needleMask.needle(), needleMask.mask());

		List<NeedleMask> needleMasksLongestFirst = new ArrayList<>(maskByNeedle.size());

		for (Map.Entry<String, String> entry : maskByNeedle.entrySet())
			needleMasksLongestFirst.add(new NeedleMask(entry.getKey(), entry.getValue()));

		needleMasksLongestFirst.sort((needleMask1, needleMask2) ->
				Integer.compare(needleMask2.needle().length(), needleMask1.needle().length()));

		List<NeedleMask> immutableNeedleMasks = List.copyOf(needleMasksLongestFirst);
		UnaryOperator<String> redactor = (text) -> text == null ? null : scrubDiagnosticText(text, immutableNeedleMasks);

		return new DiagnosticScrub(redactor, List.copyOf(needleRenderingFailureDescriptions));
	}

	private static void collectSecureNeedles(@Nullable Object parameter,
																					 @NonNull List<NeedleMask> needleMasks,
																					 @NonNull List<String> needleRenderingFailureDescriptions) {
		SecureParameter secureParameter;

		try {
			secureParameter = displaySecureParameter(parameter);
		} catch (Throwable t) {
			needleRenderingFailureDescriptions.add(t.getClass().getName());
			return;
		}

		if (secureParameter == null)
			return;

		try {
			String mask = maskOf(secureParameter);
			Object value = unwrapSecureAndOptionalParameter(parameter);
			addNeedlesForValue(value, mask, needleMasks, needleRenderingFailureDescriptions, 0);
		} catch (Throwable t) {
			// Never let a user value's toString()/getMask()/getValue() failure escape - and never attach
			// the raw throwable, whose message may itself embed the value we have no needle to scrub
			needleRenderingFailureDescriptions.add(t.getClass().getName());
		}
	}

	private static void addNeedlesForValue(@Nullable Object value,
																				 @NonNull String mask,
																				 @NonNull List<NeedleMask> needleMasks,
																				 @NonNull List<String> needleRenderingFailureDescriptions,
																				 int depth) {
		if (depth >= MAX_NESTING_DEPTH)
			return;

		try {
			// Unwrap Pyranid's typed wrappers to the innermost bound value. VectorParameter is deliberately
			// skipped: drivers render vectors as literals (e.g. "[1.0,2.0]") that never match a Java rendering.
			for (int i = 0; i < MAX_NESTING_DEPTH; ++i) {
				if (value instanceof TypedParameter typedParameter) {
					value = unwrapOptionalValue(typedParameter.getValue().orElse(null));
				} else if (value instanceof JsonParameter jsonParameter) {
					value = jsonParameter.getJson().orElse(null);
				} else {
					break;
				}
			}

			if (value instanceof InListParameter inListParameter) {
				for (Object element : inListParameter.getElements())
					addNeedlesForValue(unwrapSecureAndOptionalParameter(element), mask, needleMasks,
							needleRenderingFailureDescriptions, depth + 1);
				return;
			}

			if (value instanceof SqlArrayParameter<?> sqlArrayParameter) {
				Object[] elements = sqlArrayParameter.getElements().orElse(null);

				if (elements != null)
					for (Object element : elements)
						addNeedlesForValue(unwrapSecureAndOptionalParameter(element), mask, needleMasks,
								needleRenderingFailureDescriptions, depth + 1);
				return;
			}

			if (value == null || value instanceof Boolean || value instanceof VectorParameter)
				return;

			String needle = String.valueOf(value);

			if (needle.length() < MIN_SCRUB_NEEDLE_LENGTH)
				return;

			needleMasks.add(new NeedleMask(needle, mask));
		} catch (Throwable t) {
			needleRenderingFailureDescriptions.add(t.getClass().getName());
		}
	}

	/**
	 * Single left-to-right pass; at each position needles are tried longest-first, and on a match the
	 * needle's mask is appended and never re-scanned - so results are order-independent and immune to
	 * mask/needle interference (a mask whose text contains another needle is never re-masked).
	 * Semantics: leftmost-longest wins.
	 */
	@NonNull
	static String scrubDiagnosticText(@NonNull String text,
																		@NonNull List<NeedleMask> needleMasksLongestFirst) {
		requireNonNull(text);
		requireNonNull(needleMasksLongestFirst);

		StringBuilder scrubbed = null;
		int i = 0;

		while (i < text.length()) {
			NeedleMask match = null;

			for (NeedleMask needleMask : needleMasksLongestFirst) {
				if (text.startsWith(needleMask.needle(), i)) {
					match = needleMask;
					break;
				}
			}

			if (match == null) {
				if (scrubbed != null)
					scrubbed.append(text.charAt(i));

				++i;
			} else {
				if (scrubbed == null)
					scrubbed = new StringBuilder(text.length()).append(text, 0, i);

				scrubbed.append(match.mask());
				i += match.needle().length();
			}
		}

		return scrubbed == null ? text : scrubbed.toString();
	}

	@Nullable
	static Object unwrapSecureAndOptionalParameter(@Nullable Object parameter) {
		return unwrapSecureAndOptionalParameterWithMetadata(parameter).value();
	}

	@NonNull
	static SecureParameterUnwrapResult unwrapSecureAndOptionalParameterWithMetadata(@Nullable Object parameter) {
		Object unwrappedParameter = parameter;
		SecureParameter secureParameter = null;
		int secureParameterDepth = 0;

		while (true) {
			if (unwrappedParameter instanceof SecureParameter currentSecureParameter) {
				if (secureParameterDepth >= MAX_NESTING_DEPTH)
					throw secureParameterNestingDepthExceeded();

				++secureParameterDepth;

				if (secureParameter == null)
					secureParameter = currentSecureParameter;

				unwrappedParameter = currentSecureParameter.getValue().orElse(null);
				continue;
			}

			Object optionalValue = unwrapOptionalValue(unwrappedParameter);

			if (optionalValue == unwrappedParameter)
				return new SecureParameterUnwrapResult(secureParameter, unwrappedParameter);

			unwrappedParameter = optionalValue;
		}
	}

	@NonNull
	static String maskOf(@NonNull SecureParameter secureParameter) {
		requireNonNull(secureParameter);
		return requireNonNull(secureParameter.getMask());
	}

	@Nullable
	static SecureParameter displaySecureParameter(@Nullable Object parameter) {
		Object displayParameter = parameter;

		while (true) {
			if (displayParameter instanceof SecureParameter secureParameter)
				return secureParameter;

			Object optionalValue = unwrapOptionalValue(displayParameter);

			if (optionalValue == displayParameter)
				return null;

			displayParameter = optionalValue;
		}
	}

	@NonNull
	private static IllegalArgumentException secureParameterNestingDepthExceeded() {
		return new IllegalArgumentException(format("SecureParameter nesting exceeds the maximum depth of %s", MAX_NESTING_DEPTH));
	}

	record SecureParameterUnwrapResult(@Nullable SecureParameter secureParameter,
																		 @Nullable Object value) {}

	@Nullable
	private static Object unwrapOptionalValue(@Nullable Object value) {
		if (value == null)
			return null;

		if (value instanceof Optional<?> optional)
			return optional.orElse(null);
		if (value instanceof OptionalInt optionalInt)
			return optionalInt.isPresent() ? optionalInt.getAsInt() : null;
		if (value instanceof OptionalLong optionalLong)
			return optionalLong.isPresent() ? optionalLong.getAsLong() : null;
		if (value instanceof OptionalDouble optionalDouble)
			return optionalDouble.isPresent() ? optionalDouble.getAsDouble() : null;

		return value;
	}
}
