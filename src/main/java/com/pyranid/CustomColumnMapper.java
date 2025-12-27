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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;

/**
 * Enables per-column {@link ResultSet} mapping customization via {@link ResultSetMapper}.
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 3.0.0
 */
public interface CustomColumnMapper {
	/**
	 * Perform custom mapping of a {@link ResultSet} column: given a {@code resultSetValue}, optionally return an instance of {@code targetType} instead.
	 * <p>
	 * This method is only invoked when {@code resultSetValue} is non-null.
	 *
	 * @param statementContext current SQL context
	 * @param resultSet        the {@link ResultSet} from which data was read
	 * @param resultSetValue   the already-read value from the {@link ResultSet}, to be optionally converted to an instance of {@code targetType}
	 * @param targetType       the type to which the {@code resultSetValue} should be converted
	 * @param columnIndex      1-based column index
	 * @param columnLabel      normalized column label, if available
	 * @param instanceProvider instance-creation factory, may be used to instantiate values
	 * @return the result of the custom column mapping operation - either {@link MappingResult#of(Object)} to indicate a successfully-mapped value or {@link MappingResult#fallback()} if Pyranid should fall back to the registered {@link ResultSetMapper} mapping behavior.
	 * @throws SQLException if an error occurs during mapping
	 */
	@NonNull
	MappingResult map(@NonNull StatementContext<?> statementContext,
										@NonNull ResultSet resultSet,
										@NonNull Object resultSetValue,
										@NonNull TargetType targetType,
										@NonNull Integer columnIndex,
										@Nullable String columnLabel,
										@NonNull InstanceProvider instanceProvider) throws SQLException;

	/**
	 * Specifies which types this mapper should handle.
	 * <p>
	 * For example, if this mapper should apply when marshaling to {@code MyCustomType}, this method could return {@code targetType.matchesClass(MyCustomType.class)}.
	 * <p>
	 * For parameterized types like {@code List<UUID>}, this method could return {@code targetType.matchesParameterizedType(List.class, UUID.class)}.
	 *
	 * @param targetType the target type to evaluate - should this mapper handle it or not?
	 * @return {@code true} if this mapper should handle the type, {@code false} otherwise.
	 */
	@NonNull
	Boolean appliesTo(@NonNull TargetType targetType);

	/**
	 * Result of a custom column mapping attempt.
	 * <p>
	 * Use {@link #of(Object)} to indicate a successfully mapped value or {@link #fallback()} to indicate "didn't map; fall back to the registered {@link ResultSetMapper} behavior".</p>
	 */
	@ThreadSafe
	sealed abstract class MappingResult permits MappingResult.CustomMapping, MappingResult.Fallback {
		private MappingResult() {}

		/**
		 * Indicates a successfully-mapped custom value.
		 *
		 * @param value the custom value, may be {@code null}
		 * @return a result which indicates a successfully-mapped custom value
		 */
		@NonNull
		public static MappingResult of(@Nullable Object value) {
			return new CustomMapping(value);
		}

		/**
		 * Indicates that this mapper did not map a custom value and prefers to fall back to the behavior of the registered {@link ResultSetMapper}.
		 *
		 * @return a result which indicates that this mapper did not map a custom value
		 */
		@NonNull
		public static MappingResult fallback() {
			return Fallback.INSTANCE;
		}

		@ThreadSafe
		static final class CustomMapping extends MappingResult {
			@Nullable
			private final Object value;

			private CustomMapping(@Nullable Object value) {
				this.value = value;
			}

			@NonNull
			public Optional<Object> getValue() {
				return Optional.ofNullable(value);
			}
		}

		@ThreadSafe
		static final class Fallback extends MappingResult {
			static final Fallback INSTANCE = new Fallback();

			private Fallback() {}
		}
	}
}