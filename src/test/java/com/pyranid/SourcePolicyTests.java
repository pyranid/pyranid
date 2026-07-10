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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static java.lang.String.format;

/**
 * Source-level policy guards for properties that are invisible to functional tests.
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 */
public class SourcePolicyTests {
	private static final Pattern SYNCHRONIZED_PATTERN = Pattern.compile("\\bsynchronized\\b");

	/**
	 * Pyranid's main source deliberately contains zero {@code synchronized} - blocking while holding a monitor
	 * pins virtual threads to their carriers, and the library's lock-based design ({@code ReentrantLock},
	 * {@code ConcurrentHashMap}) is what makes it a clean virtual-thread citizen. A reintroduced
	 * {@code synchronized} in a hot path would pass every functional test and surface only as throughput
	 * collapse under load. This test converts the property into a maintained contract.
	 * <p>
	 * If a {@code synchronized} usage is ever genuinely required, replace it with {@code ReentrantLock}
	 * (parking on a j.u.c lock unmounts a virtual thread; parking inside a monitor does not).
	 */
	@Test
	public void testMainSourceContainsNoSynchronized() throws IOException {
		Path mainSourceRoot = Path.of("src", "main", "java");
		Assertions.assertTrue(Files.isDirectory(mainSourceRoot),
				"Main source root not found; test must run from the module root");

		List<String> violations = new ArrayList<>();

		try (Stream<Path> paths = Files.walk(mainSourceRoot)) {
			paths.filter(path -> path.toString().endsWith(".java")).forEach(path -> {
				String source;
				try {
					source = Files.readString(path);
				} catch (IOException e) {
					throw new RuntimeException(e);
				}

				String[] lines = stripCommentsAndStrings(source).split("\n", -1);

				for (int i = 0; i < lines.length; ++i)
					if (SYNCHRONIZED_PATTERN.matcher(lines[i]).find())
						violations.add(format("%s:%d", path, i + 1));
			});
		}

		Assertions.assertTrue(violations.isEmpty(), () -> format(
				"Found 'synchronized' in main source - this pins virtual threads to carrier threads. "
						+ "Use ReentrantLock instead. Violations: %s", violations));
	}

	/**
	 * Removes block comments, line comments, string literals, char literals, and text blocks while preserving
	 * line structure, so keyword scans see only live code.
	 */
	private static String stripCommentsAndStrings(String source) {
		StringBuilder stripped = new StringBuilder(source.length());
		int i = 0;
		int length = source.length();

		while (i < length) {
			char c = source.charAt(i);
			char next = i + 1 < length ? source.charAt(i + 1) : '\0';

			if (c == '/' && next == '*') {
				// Block comment (preserve newlines for line numbering)
				i += 2;
				while (i < length && !(source.charAt(i) == '*' && i + 1 < length && source.charAt(i + 1) == '/')) {
					if (source.charAt(i) == '\n')
						stripped.append('\n');
					++i;
				}
				i += 2;
			} else if (c == '/' && next == '/') {
				// Line comment
				while (i < length && source.charAt(i) != '\n')
					++i;
			} else if (c == '"' && next == '"' && i + 2 < length && source.charAt(i + 2) == '"') {
				// Text block
				i += 3;
				while (i + 2 < length && !(source.charAt(i) == '"' && source.charAt(i + 1) == '"' && source.charAt(i + 2) == '"')) {
					if (source.charAt(i) == '\n')
						stripped.append('\n');
					++i;
				}
				i += 3;
			} else if (c == '"') {
				// String literal
				++i;
				while (i < length && source.charAt(i) != '"') {
					if (source.charAt(i) == '\\')
						++i;
					++i;
				}
				++i;
			} else if (c == '\'') {
				// Char literal
				++i;
				while (i < length && source.charAt(i) != '\'') {
					if (source.charAt(i) == '\\')
						++i;
					++i;
				}
				++i;
			} else {
				stripped.append(c);
				++i;
			}
		}

		return stripped.toString();
	}
}
