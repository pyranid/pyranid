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
import javax.annotation.concurrent.ThreadSafe;
import java.util.Optional;

/**
 * Encapsulates {@link java.sql.PreparedStatement} parameter data meant to be bound to a DBMS-specific type (for example, {@code JSON} or {@code JSONB} for PostgreSQL) by {@link PreparedStatementBinder}.
 * <p>
 * Stardard instances may be constructed via {@link Parameters#json(String)} and {@link Parameters#json(String, BindingPreference)}.
 * <p>
 * Implementations should be threadsafe.
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 3.0.0
 */
@ThreadSafe
public interface JsonParameter {
	/**
	 * Specifies how a {@link JsonParameter} should be bound - DBMS-specific sensible default, binary (for example, {@code JSONB} for PostgreSQL), or text.
	 *
	 * @author <a href="https://www.revetkn.com">Mark Allen</a>
	 * @since 3.0.0
	 */
	enum BindingPreference {
		/**
		 * Prefer the most capable native type for the target DB (for example, {@code JSONB} for PostgreSQL), else text.
		 */
		AUTOMATIC,
		/**
		 * Prefer a binary/native JSON type when available (for example, {@code JSONB} for PostgreSQL), else text.
		 */
		BINARY,
		/**
		 * Prefer to bind as text (for example, {@code VARCHAR}/{@code TEXT}/{@code NVARCHAR} or {@code JSON} for PostgreSQL).
		 */
		TEXT
	}

	/**
	 * Gets the "stringified" JSON.
	 *
	 * @return the "stringified" JSON
	 */
	@Nonnull
	Optional<String> getJson();

	/**
	 * Gets how the JSON should be bound (automatic, binary, text).
	 *
	 * @return how the JSON should be bound
	 */
	@Nonnull
	BindingPreference getBindingPreference();
}