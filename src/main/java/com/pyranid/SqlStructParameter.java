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

import javax.annotation.concurrent.ThreadSafe;
import java.util.List;
import java.util.Optional;

/**
 * Encapsulates {@link java.sql.PreparedStatement} parameter data meant to be bound to a formal
 * {@link java.sql.Struct} by {@link PreparedStatementBinder}.
 * <p>
 * The type name is database-specific and is passed to
 * {@link java.sql.Connection#createStruct(String, Object[])}. For example, DuckDB accepts an inline
 * type such as {@code STRUCT(name VARCHAR, age INTEGER)}. Attributes are positional and may contain
 * {@code null}, nested {@code SqlStructParameter}s, or {@link SqlArrayParameter}s.
 * <p>
 * DuckDB attributes that represent an instant require a complete inline STRUCT declaration so Pyranid
 * can distinguish {@code TIMESTAMP} from {@code TIMESTAMPTZ}. A named {@code CREATE TYPE} alias does not
 * expose its attribute types through this binding path, so binding an instant-bearing value through one
 * fails fast rather than guessing its timestamp semantics.
 * <p>
 * Standard instances may be constructed via {@link Parameters#sqlStructOf(String, Object[])} and
 * {@link Parameters#sqlStructOf(String, List)}.
 * <p>
 * Implementations should be threadsafe.
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 4.7.0
 */
@ThreadSafe
public interface SqlStructParameter {
	/**
	 * Gets the database-specific SQL STRUCT type name.
	 *
	 * @return the SQL STRUCT type name
	 */
	@NonNull
	String getTypeName();

	/**
	 * Gets the positional attributes of this SQL STRUCT.
	 *
	 * @return the attributes, or {@link Optional#empty()} when the SQL STRUCT itself is {@code NULL}
	 */
	@NonNull
	Optional<Object[]> getAttributes();
}
