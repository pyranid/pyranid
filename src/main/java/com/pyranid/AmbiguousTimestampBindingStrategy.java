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

/**
 * Controls how {@link java.time.Instant} and {@link java.time.OffsetDateTime} parameters are bound when JDBC
 * {@link java.sql.ParameterMetaData} cannot identify whether the target column is {@code TIMESTAMP} or
 * {@code TIMESTAMP WITH TIME ZONE}.
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 4.2.0
 */
public enum AmbiguousTimestampBindingStrategy {
	/**
	 * Bind ambiguous {@link java.time.Instant} and {@link java.time.OffsetDateTime} parameters as
	 * {@code TIMESTAMP WITH TIME ZONE}.
	 * <p>
	 * This is Pyranid's default strategy and is appropriate when ambiguous parameters target timestamp-with-time-zone
	 * columns.
	 */
	TIMESTAMP_WITH_TIME_ZONE,

	/**
	 * Bind ambiguous {@link java.time.Instant} and {@link java.time.OffsetDateTime} parameters as {@code TIMESTAMP}
	 * without time zone.
	 * <p>
	 * Pyranid converts {@link java.time.Instant} and {@link java.time.OffsetDateTime} parameters to the
	 * {@link Database.Builder#timeZone(java.time.ZoneId)} zone and binds the resulting local timestamp with
	 * {@link java.sql.Types#TIMESTAMP}.
	 */
	TIMESTAMP_WITHOUT_TIME_ZONE
}
