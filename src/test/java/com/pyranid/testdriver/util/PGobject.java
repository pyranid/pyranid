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

package com.pyranid.testdriver.util;

/**
 * Stands in for a PostgreSQL JDBC driver fork's {@code PGobject}, mirroring the class name and
 * {@code ...util} package shape that pgjdbc and its forks (for example YugabyteDB's smart driver, which
 * ships {@code com.yugabyte.util.PGobject}) share. Used to verify that Pyranid resolves the driver's own
 * type rather than pgjdbc's by name.
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 4.7.0
 */
public class PGobject {
	private static final ThreadLocal<Throwable> NEXT_CONSTRUCTION_FAILURE = new ThreadLocal<>();
	private static final ThreadLocal<Throwable> NEXT_TYPE_SETTER_FAILURE = new ThreadLocal<>();
	private static final ThreadLocal<Throwable> NEXT_VALUE_SETTER_FAILURE = new ThreadLocal<>();

	private String type;
	private String value;
	private final Throwable valueReaderFailure;

	public PGobject() {
		this.valueReaderFailure = null;
		throwIfPresent(take(NEXT_CONSTRUCTION_FAILURE));
	}

	public PGobject(Throwable valueReaderFailure) {
		this.valueReaderFailure = valueReaderFailure;
	}

	public static void failNextConstructionWith(Throwable failure) {
		NEXT_CONSTRUCTION_FAILURE.set(failure);
	}

	public static void failNextTypeSetWith(Throwable failure) {
		NEXT_TYPE_SETTER_FAILURE.set(failure);
	}

	public static void failNextValueSetWith(Throwable failure) {
		NEXT_VALUE_SETTER_FAILURE.set(failure);
	}

	public static void clearInjectedFailures() {
		NEXT_CONSTRUCTION_FAILURE.remove();
		NEXT_TYPE_SETTER_FAILURE.remove();
		NEXT_VALUE_SETTER_FAILURE.remove();
	}

	public String getType() {
		return this.type;
	}

	public void setType(String type) {
		throwIfPresent(take(NEXT_TYPE_SETTER_FAILURE));
		this.type = type;
	}

	public String getValue() {
		throwIfPresent(this.valueReaderFailure);
		return this.value;
	}

	public void setValue(String value) {
		throwIfPresent(take(NEXT_VALUE_SETTER_FAILURE));
		this.value = value;
	}

	private static Throwable take(ThreadLocal<Throwable> failureHolder) {
		Throwable failure = failureHolder.get();
		failureHolder.remove();
		return failure;
	}

	private static void throwIfPresent(Throwable failure) {
		if (failure != null)
			PGobject.<RuntimeException>throwUnchecked(failure);
	}

	@SuppressWarnings("unchecked")
	private static <T extends Throwable> void throwUnchecked(Throwable failure) throws T {
		throw (T) failure;
	}
}
