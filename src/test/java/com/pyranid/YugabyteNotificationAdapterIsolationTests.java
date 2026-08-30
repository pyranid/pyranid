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
import org.junit.jupiter.api.io.TempDir;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Verifies the explicitly supported YugabyteDB smart-driver notification API without putting either supported
 * PostgreSQL driver on the isolated child JVM's application classpath.
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 4.7.0
 */
public class YugabyteNotificationAdapterIsolationTests {
	@TempDir
	private Path temporaryDirectory;

	@Test
	public void testYugabyteApiIsResolvedThroughTheContextLoader() throws Exception {
		Path driverClasses = compileDriverApi();
		String isolatedClasspath = Arrays.stream(
						System.getProperty("java.class.path").split(File.pathSeparator))
				.filter(classpathEntry -> !isSupportedPostgresDriverJar(classpathEntry))
				.collect(Collectors.joining(File.pathSeparator));
		Path javaExecutable = Path.of(
				System.getProperty("java.home"), "bin", isWindows() ? "java.exe" : "java");
		Process process = new ProcessBuilder(
				javaExecutable.toString(),
				"-cp",
				isolatedClasspath,
				YugabyteNotificationAdapterClasspathProbe.class.getName(),
				driverClasses.toString())
				.redirectErrorStream(true)
				.start();
		boolean completed = process.waitFor(20L, TimeUnit.SECONDS);

		if (!completed)
			process.destroyForcibly();

		String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
		Assertions.assertTrue(completed, () -> "Isolated YugabyteDB probe did not terminate. Output:\n" + output);
		Assertions.assertEquals(0, process.exitValue(), () ->
				"Isolated YugabyteDB probe failed. Output:\n" + output);
	}

	private Path compileDriverApi() throws Exception {
		JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
		Assertions.assertNotNull(compiler, "Tests must run on a JDK, not a JRE");
		Path sourceDirectory = this.temporaryDirectory.resolve("source").resolve("com").resolve("yugabyte");
		Path classDirectory = this.temporaryDirectory.resolve("classes");
		Files.createDirectories(sourceDirectory);
		Files.createDirectories(classDirectory);
		Path connectionSource = sourceDirectory.resolve("PGConnection.java");
		Path notificationSource = sourceDirectory.resolve("PGNotification.java");

		Files.writeString(connectionSource, """
				package com.yugabyte;

				import java.sql.SQLException;

				public interface PGConnection {
					PGNotification[] getNotifications() throws SQLException;
					PGNotification[] getNotifications(int timeoutMilliseconds) throws SQLException;
				}
				""", StandardCharsets.UTF_8);
		Files.writeString(notificationSource, """
				package com.yugabyte;

				public interface PGNotification {
					String getName();
					String getParameter();
				}
				""", StandardCharsets.UTF_8);

		int compilationResult = compiler.run(null, null, null,
				"-d", classDirectory.toString(), connectionSource.toString(), notificationSource.toString());
		Assertions.assertEquals(0, compilationResult, "Unable to compile isolated YugabyteDB driver API fixtures");
		return classDirectory;
	}

	private static boolean isSupportedPostgresDriverJar(String classpathEntry) {
		String filename = Path.of(classpathEntry).getFileName().toString().toLowerCase();
		return filename.startsWith("postgresql-") && filename.endsWith(".jar")
				|| filename.contains("yugabyte") && filename.endsWith(".jar");
	}

	private static boolean isWindows() {
		return System.getProperty("os.name").toLowerCase().contains("win");
	}
}
