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
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * Enables per-column {@link ResultSet} mapping customization via {@link ResultSetMapper}.
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 2.1.0
 */
public interface CustomColumnMapper {
	/**
	 * Perform custom mapping of a {@link ResultSet} column: given a {@code resultSetValue}, optionally return an instance of {@code targetType} instead.
	 * <p>
	 * This function is only invoked when {@code resultSetValue} is non-null.
	 *
	 * @param statementContext current SQL context
	 * @param resultSet        the {@link ResultSet} from which data was read
	 * @param resultSetValue   the already-read value from the {@link ResultSet}, to be optionally converted to an instance of {@code targetType}
	 * @param targetType       the type to which the {@code resultSetValue} should be converted
	 * @param columnIndex      1-based column index, if available
	 * @param columnLabel      normalized column label, if available
	 * @param instanceProvider instance-creation factory, may be used to instantiate values
	 * @return an {@link Optional} which holds the preferred value for this {@link ResultSet} column, or {@link Optional#empty()} to fall back to default mapping
	 */
	@Nonnull
	Optional<?> map(@Nonnull StatementContext<?> statementContext,
									@Nonnull ResultSet resultSet,
									@Nonnull Object resultSetValue,
									@Nonnull TargetType targetType,
									@Nullable Integer columnIndex,
									@Nullable String columnLabel,
									@Nonnull InstanceProvider instanceProvider) throws SQLException;

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
	@Nonnull
	Boolean appliesTo(@Nonnull TargetType targetType);
}