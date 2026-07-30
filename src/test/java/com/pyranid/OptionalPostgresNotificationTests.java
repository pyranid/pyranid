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

import javax.annotation.concurrent.ThreadSafe;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Verifies optional pgjdbc behavior in a genuinely isolated child-JVM classpath.
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 4.6.0
 */
@ThreadSafe
public class OptionalPostgresNotificationTests {
	@Test
	public void coreAndPureSqlSendLoadWithoutPostgresDriver() throws Exception {
		String isolatedClasspath = Arrays.stream(
						System.getProperty("java.class.path").split(File.pathSeparator))
				.filter(classpathEntry -> !isPostgresDriverJar(classpathEntry))
				.collect(Collectors.joining(File.pathSeparator));
		Path javaExecutable = Path.of(
				System.getProperty("java.home"), "bin", isWindows() ? "java.exe" : "java");
		Process process = new ProcessBuilder(
				javaExecutable.toString(),
				"-cp",
				isolatedClasspath,
				OptionalPostgresNotificationClasspathProbe.class.getName())
				.redirectErrorStream(true)
				.start();
		boolean completed = process.waitFor(20L, TimeUnit.SECONDS);

		if (!completed)
			process.destroyForcibly();

		String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
		Assertions.assertTrue(completed, () -> "Isolated child JVM did not terminate. Output:\n" + output);
		Assertions.assertEquals(0, process.exitValue(), () ->
				"Isolated child JVM failed. Output:\n" + output);
	}

	private static boolean isPostgresDriverJar(String classpathEntry) {
		String filename = Path.of(classpathEntry).getFileName().toString();
		return filename.startsWith("postgresql-") && filename.endsWith(".jar");
	}

	private static boolean isWindows() {
		return System.getProperty("os.name").toLowerCase().contains("win");
	}
}
