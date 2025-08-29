/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2025 Revetware LLC.
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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Encapsulates prepared-statement parameter data meant to be bound to a DBMS-specific type (e.g. {@code JSON} or {@code JSONB} for PostgreSQL) by {@link PreparedStatementBinder}.
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 2.1.0
 */
@ThreadSafe
public final class JsonParameter {
	@Nullable
	private final String json;
	@Nonnull
	private final BindingPreference bindingPreference;

	private JsonParameter(@Nullable String json,
												@Nonnull BindingPreference bindingPreference) {
		requireNonNull(bindingPreference);

		this.json = json;
		this.bindingPreference = bindingPreference;
	}

	/**
	 * Acquires a JSON parameter for "stringified" JSON.
	 *
	 * @param json              the stringified JSON for this parameter
	 * @param bindingPreference how the JSON parameter should be bound to a {@link java.sql.PreparedStatement}
	 * @return the JSON parameter
	 */
	@Nonnull
	public static JsonParameter of(@Nullable String json,
																 @Nonnull BindingPreference bindingPreference) {
		requireNonNull(bindingPreference);

		return new JsonParameter(json, bindingPreference);
	}

	/**
	 * Specifies how a {@link JsonParameter} should be bound - DBMS-specific sensible default, binary (e.g. {@code JSONB} for PostgreSQL), or text.
	 *
	 * @author <a href="https://www.revetkn.com">Mark Allen</a>
	 * @since 2.1.0
	 */
	public enum BindingPreference {
		/**
		 * Prefer the most capable native type for the target DB (e.g. {@code JSONB} for PostgreSQL), else text.
		 */
		AUTOMATIC,
		/**
		 * Prefer a binary/native JSON type when available (e.g. {@code JSONB} for PostgreSQL), else text.
		 */
		BINARY,
		/**
		 * Prefer to bind as text (e.g. {@code VARCHAR}/{@code TEXT}/{@code NVARCHAR} or {@code JSON} for PostgreSQL).
		 */
		TEXT
	}

	@Nonnull
	public Optional<String> getJson() {
		return Optional.ofNullable(this.json);
	}

	@Nonnull
	public BindingPreference getBindingPreference() {
		return this.bindingPreference;
	}
}