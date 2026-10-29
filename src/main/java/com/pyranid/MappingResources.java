/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.pyranid;

import org.jspecify.annotations.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.lang.ref.WeakReference;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Struct;
import java.sql.Types;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;

/** Per-row ownership and bounded, memoized materialization. Never retained by a mapping plan. */
final class MappingResources implements AutoCloseable {
	private final long maxBytes;
	private final long maxCharacters;
	private final @Nullable MappingResources parent;
	private @Nullable Map<Object, Object> materialized;
	private @Nullable Map<Object, Object> arrayTypes;
	private @Nullable Map<Object, String> structTypes;
	private @Nullable Set<Object> owned;
	private @Nullable Set<Object> borrowed;
	private boolean opaqueBorrow;
	private boolean inUse;
	private @Nullable MappingResources nested;
	private @Nullable WeakReference<ResultSet> scalarResultSet;
	private int scalarJdbcType;

	/** Reuse only empty scopes; recursive mapper calls receive an independent scope. */
	MappingResources acquire() {
		if (!this.inUse) {
			this.inUse = true;
			return this;
		}
		if (this.nested == null) this.nested = new MappingResources(this.maxBytes, this.maxCharacters, this);
		return this.nested.acquire();
	}

	int scalarJdbcType(ResultSet resultSet, ResultSetMetaData metadata) throws SQLException {
		if (this.scalarResultSet == null || this.scalarResultSet.get() != resultSet) {
			this.scalarJdbcType = metadata.getColumnType(1);
			this.scalarResultSet = new WeakReference<>(resultSet);
		}
		return this.scalarJdbcType;
	}

	void registerArrayType(@Nullable Object value, int jdbcType) {
		if (value == null || !value.getClass().isArray() || jdbcType == Types.OTHER || jdbcType == Types.ARRAY) return;
		if (this.arrayTypes == null) this.arrayTypes = new IdentityHashMap<>();
		Object previous = this.arrayTypes.get(value);
		this.arrayTypes.put(value, previous instanceof TimeArrayValues timeValues
				? new TimeArrayValues(jdbcType, timeValues.valuesOrFailure) : jdbcType);
	}

	int arrayType(Object value) {
		Object metadata = this.arrayTypes == null ? null : this.arrayTypes.get(value);
		return metadata instanceof Integer type ? type : metadata instanceof TimeArrayValues times ? times.jdbcType : Types.OTHER;
	}

	void copyArrayType(Object original, @Nullable Object converted) {
		if (converted != null && converted != original && converted.getClass().isArray()
				&& this.arrayTypes != null && this.arrayTypes.containsKey(original))
			this.arrayTypes.put(converted, this.arrayTypes.get(original));
	}

	private record TimeArrayValues(int jdbcType, Object valuesOrFailure) {}

	void registerArrayTimeValues(Object original, Object valuesOrFailure) {
		int jdbcType = arrayType(original);
		if (this.arrayTypes == null) this.arrayTypes = new IdentityHashMap<>();
		this.arrayTypes.put(original, new TimeArrayValues(jdbcType, valuesOrFailure));
	}

	Object arrayTimeValues(Object original) {
		Object metadata = this.arrayTypes == null ? null : this.arrayTypes.get(original);
		if (!(metadata instanceof TimeArrayValues times)) return original;
		if (times.valuesOrFailure instanceof Throwable failure)
			throw DatabaseException.wrapSafely("Driver cannot read SQL TIME array elements without losing precision or offset", failure,
					GenericDialect.INSTANCE);
		return times.valuesOrFailure;
	}

	void registerStructType(Object value, @Nullable String typeName) {
		if (typeName == null) return;
		if (this.structTypes == null) this.structTypes = new IdentityHashMap<>();
		this.structTypes.put(value, typeName);
	}

	@Nullable String structType(Object value) {
		return this.structTypes == null ? null : this.structTypes.get(value);
	}

	void copyStructType(Object original, Object converted) {
		if (converted != original) registerStructType(converted, structType(original));
	}

	static boolean requiresMaterialization(@Nullable Object raw, Class<?> target, int jdbcType) {
		return (target == byte[].class && (raw instanceof Blob || jdbcType == Types.BLOB))
				|| (target == String.class && (raw instanceof Clob || jdbcType == Types.CLOB || jdbcType == Types.NCLOB));
	}

	MappingResources(@Nullable Long maxBytes, @Nullable Long maxCharacters) {
		this(maxBytes, maxCharacters, null);
	}

	private MappingResources(@Nullable Long maxBytes, @Nullable Long maxCharacters, @Nullable MappingResources parent) {
		this.parent = parent;
		// Even an explicitly unbounded Java value must fit in its array/string representation.
		this.maxBytes = Math.min(maxBytes == null ? Long.MAX_VALUE : maxBytes, Integer.MAX_VALUE - 8L);
		this.maxCharacters = Math.min(maxCharacters == null ? Long.MAX_VALUE : maxCharacters, Integer.MAX_VALUE - 8L);
	}

	void borrow(@Nullable Object value) {
		if (!(value instanceof Blob || value instanceof Clob || value instanceof Struct || value instanceof Array
				|| value instanceof Object[] || value instanceof Iterable<?> || value instanceof Map<?, ?>)) return;
		// A nested custom hook can retain a handle already owned by an active outer row.
		if (this.parent != null) this.parent.borrow(value);
		if (this.borrowed == null) this.borrowed = Collections.newSetFromMap(new IdentityHashMap<>());
		if (!this.borrowed.add(value)) return;
		if (this.owned != null) this.owned.remove(value);
		if (value instanceof Struct || value instanceof Array || value instanceof Iterable<?> || value instanceof Map<?, ?>) {
			// Traversing application containers can consume one-shot results or execute arbitrary
			// user code. They may retain any row handle, so leave cleanup to the owning cursor.
			this.opaqueBorrow = true;
			if (this.owned != null) this.owned.clear();
		} else if (value instanceof Object[] elements) {
			for (Object element : elements) borrow(element);
		}
	}

	@Nullable Object materialize(@Nullable Object raw, Class<?> target, int jdbcType, String location) {
		return materialize(raw, target, jdbcType, location, true);
	}

	private @Nullable Object materialize(@Nullable Object raw, Class<?> target, int jdbcType, String location, boolean bounded) {
		if (raw == null)
			return null;
		boolean binary = target == byte[].class && (raw instanceof Blob || jdbcType == Types.BLOB);
		boolean character = target == String.class && (raw instanceof Clob || jdbcType == Types.CLOB || jdbcType == Types.NCLOB);
		if (!binary && !character)
			return raw;
		long byteLimit = bounded ? this.maxBytes : Integer.MAX_VALUE - 8L;
		long characterLimit = bounded ? this.maxCharacters : Integer.MAX_VALUE - 8L;
		if (this.materialized != null && this.materialized.containsKey(raw)) {
			Object existing = this.materialized.get(raw);
			// Native/dialect results may have been materialized without a typed-target limit.
			if (binary && existing instanceof byte[] bytes) checkLength(bytes.length, byteLimit, location, "byte[]", "bytes");
			if (character && existing instanceof String string) checkLength(string.length(), characterLimit, location, "String", "UTF-16 code units");
			return existing;
		}

		Object value = raw;
		try {
			if (raw instanceof Blob blob && binary) {
				own(blob);
				try (InputStream input = blob.getBinaryStream()) {
					value = readBytes(input, location, byteLimit);
				}
			} else if (raw instanceof Clob clob && character) {
				own(clob);
				try (Reader reader = clob.getCharacterStream()) {
					value = readCharacters(reader, location, characterLimit);
				}
			} else if (raw instanceof byte[] bytes && binary) {
				checkLength(bytes.length, byteLimit, location, "byte[]", "bytes");
			} else if (raw instanceof String string && character) {
				checkLength(string.length(), characterLimit, location, "String", "UTF-16 code units");
			}
		} catch (SQLException | IOException failure) {
			throw new DatabaseException("Unable to materialize LOB at " + location + " as " + target.getTypeName(), failure);
		}
		if (this.materialized == null) this.materialized = new IdentityHashMap<>();
		this.materialized.put(raw, value);
		return value;
	}

	/** Native Map/Object targets keep dialect normalization without changing the typed LOB policy. */
	@Nullable Object nativeValue(@Nullable Object raw, DatabaseDialect dialect) {
		if (raw == null) return null;
		borrow(raw);
		if (!(dialect instanceof DuckDbDialect duck)) return dialect.unwrapResultSetValue(raw);
		return nativeDuckValue(duck.unwrapForTypedMapping(raw));
	}

	private @Nullable Object nativeDuckValue(@Nullable Object value) {
		if (value == null) return null;
		if (value instanceof Blob && value.getClass().getName().startsWith("org.duckdb.")) {
			// DuckDB's native normalization has always exposed BLOBs as byte[]. Reuse the row's
			// detached value, but leave borrowed handles to their cursor rather than freeing twice.
			borrow(value);
			return materialize(value, byte[].class, Types.BLOB, "native DuckDB value", false);
		}
		if (value instanceof Object[] elements) {
			Object[] result = elements;
			for (int i = 0; i < elements.length; i++) {
				Object converted = nativeDuckValue(elements[i]);
				if (result == elements && converted != elements[i]) result = elements.clone();
				if (result != elements) result[i] = converted;
			}
			return result;
		}
		if (value instanceof List<?> elements) {
			List<Object> result = new ArrayList<>(elements.size());
			boolean changed = false;
			for (Object element : elements) {
				Object converted = nativeDuckValue(element);
				result.add(converted);
				changed |= converted != element;
			}
			return changed ? result : elements;
		}
		if (value instanceof Map<?, ?> entries) {
			Map<Object, Object> result = new LinkedHashMap<>(entries.size());
			boolean changed = false;
			for (Map.Entry<?, ?> entry : entries.entrySet()) {
				Object key = nativeDuckValue(entry.getKey());
				Object mapped = nativeDuckValue(entry.getValue());
				result.put(key, mapped);
				changed |= key != entry.getKey() || mapped != entry.getValue();
			}
			return changed ? result : entries;
		}
		return value;
	}

	private void own(Object resource) {
		if (!isBorrowed(resource) && !isOwnedByAncestor(resource)) {
			if (this.owned == null) this.owned = Collections.newSetFromMap(new IdentityHashMap<>());
			this.owned.add(resource);
		}
	}

	private boolean isBorrowed(Object resource) {
		return this.opaqueBorrow || (this.borrowed != null && this.borrowed.contains(resource))
				|| (this.parent != null && this.parent.isBorrowed(resource));
	}

	private boolean isOwnedByAncestor(Object resource) {
		return this.parent != null && ((this.parent.owned != null && this.parent.owned.contains(resource))
				|| this.parent.isOwnedByAncestor(resource));
	}

	private byte[] readBytes(InputStream input, String location, long limit) throws IOException {
		ByteArrayOutputStream output = new ByteArrayOutputStream();
		byte[] buffer = new byte[8192];
		long count = 0;
		while (true) {
			int read = input.read(buffer, 0, (int) Math.min(buffer.length, limit - count + 1));
			if (read < 0) break;
			if (read == 0) {
				int single = input.read();
				if (single < 0) break;
				buffer[0] = (byte) single;
				read = 1;
			}
			count += read;
			checkLength(count, limit, location, "byte[]", "bytes");
			output.write(buffer, 0, read);
		}
		return output.toByteArray();
	}

	private String readCharacters(Reader input, String location, long limit) throws IOException {
		StringBuilder output = new StringBuilder();
		char[] buffer = new char[8192];
		long count = 0;
		while (true) {
			int read = input.read(buffer, 0, (int) Math.min(buffer.length, limit - count + 1));
			if (read < 0) break;
			if (read == 0) {
				int single = input.read();
				if (single < 0) break;
				buffer[0] = (char) single;
				read = 1;
			}
			count += read;
			checkLength(count, limit, location, "String", "UTF-16 code units");
			output.append(buffer, 0, read);
		}
		return output.toString();
	}

	private static void checkLength(long length, long limit, String location, String target, String units) {
		if (length > limit)
			throw new DatabaseException("LOB at " + location + " exceeds configured materialization limit of "
					+ limit + " " + units + " for " + target);
	}

	@Override
	public void close() throws SQLException {
		Throwable primary = null;
		try {
			if (this.owned != null) {
				for (Object resource : this.owned) {
					try {
						if (resource instanceof Blob blob) blob.free();
						else if (resource instanceof Clob clob) clob.free();
					} catch (SQLException | RuntimeException | Error failure) {
						if (primary == null) primary = failure;
						else if (failure != primary) primary.addSuppressed(failure);
					}
				}
			}
		} finally {
			// Do not retain JDBC handles, detached values, user classes, or large row containers.
			this.owned = null;
			this.borrowed = null;
			this.materialized = null;
			this.arrayTypes = null;
			this.structTypes = null;
			this.opaqueBorrow = false;
			this.inUse = false;
		}
		if (primary instanceof SQLException sql) throw sql;
		if (primary instanceof RuntimeException runtime) throw runtime;
		if (primary instanceof Error error) throw error;
	}
}
