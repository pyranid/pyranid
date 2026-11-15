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
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.lang.ref.WeakReference;
import java.lang.reflect.Modifier;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Struct;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;

/** Per-row ownership and bounded, memoized materialization. Never retained by a mapping plan. */
final class MappingResources implements AutoCloseable {
	private final long maxBytes;
	private final long maxCharacters;
	private final boolean detachLobValues;
	private final @Nullable MappingResources parent;
	private @Nullable Map<Object, Object> materialized;
	private @Nullable Map<Object, Object> detachedContainers;
	private @Nullable Map<Object, Object> arrayTypes;
	private @Nullable Map<Array, SqlArrayExtraction> sqlArrayExtractions;
	private @Nullable Map<Object, String> containerTypeNames;
	private @Nullable Set<Object> owned;
	private @Nullable Set<Object> borrowed;
	private boolean opaqueBorrow;
	private boolean inUse;
	private @Nullable MappingResources nested;
	private @Nullable WeakReference<ResultSet> scalarResultSet;
	private int scalarJdbcType;
	private @Nullable WeakReference<ResultSet> intervalResultSet;
	private @Nullable Map<Integer, SqlIntervalSupport.SourceColumn> intervalColumns;

	/** Reuse only empty scopes; recursive mapper calls receive an independent scope. */
	MappingResources acquire() {
		if (!this.inUse) {
			this.inUse = true;
			return this;
		}
		if (this.nested == null) this.nested = new MappingResources(this.maxBytes, this.maxCharacters, this.detachLobValues, this);
		return this.nested.acquire();
	}

	int scalarJdbcType(ResultSet resultSet, ResultSetMetaData metadata) throws SQLException {
		if (this.scalarResultSet == null || this.scalarResultSet.get() != resultSet) {
			int jdbcType = metadata.getColumnType(1);
			WeakReference<ResultSet> cursor = new WeakReference<>(resultSet);
			this.scalarJdbcType = jdbcType;
			this.scalarResultSet = cursor;
		}
		return this.scalarJdbcType;
	}

	/** Immutable interval declarations can outlive a row; the cursor identity remains weak. */
	SqlIntervalSupport.SourceColumn intervalColumn(ResultSet resultSet, @Nullable ResultSetMetaData metadata,
			int column) throws SQLException {
		boolean sameCursor = this.intervalResultSet != null && this.intervalResultSet.refersTo(resultSet);
		SqlIntervalSupport.SourceColumn cached = sameCursor && this.intervalColumns != null ? this.intervalColumns.get(column) : null;
		if (cached != null) return cached;
		// Do not publish a new cursor or a partial column after a metadata getter fails. Handled
		// custom hooks never request this descriptor, so they gain no interval metadata demands.
		SqlIntervalSupport.SourceColumn source = SqlIntervalSupport.sourceColumn(metadata == null ? resultSet.getMetaData() : metadata, column);
		if (!sameCursor) {
			this.intervalColumns = new HashMap<>();
			this.intervalResultSet = new WeakReference<>(resultSet);
		}
		this.intervalColumns.put(column, source);
		return source;
	}

	void registerArrayType(@Nullable Object value, int jdbcType) {
		if (value == null || (!value.getClass().isArray() && !(value instanceof Collection<?>))
				|| jdbcType == Types.OTHER || jdbcType == Types.ARRAY) return;
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
		if (converted != null && converted != original && (converted.getClass().isArray() || converted instanceof Collection<?>)
				&& this.arrayTypes != null && this.arrayTypes.containsKey(original))
			this.arrayTypes.put(converted, this.arrayTypes.get(original));
	}

	private record TimeArrayValues(int jdbcType, Object valuesOrFailure) {}

	record SqlArrayExtraction(@Nullable Object values, @Nullable Object typeMetadata, @Nullable Throwable failure) {}

	@Nullable SqlArrayExtraction sqlArrayExtraction(Array array) {
		SqlArrayExtraction extraction = this.parent != null ? this.parent.sqlArrayExtraction(array)
				: this.sqlArrayExtractions == null ? null : this.sqlArrayExtractions.get(array);
		if (extraction != null && extraction.values != null && extraction.typeMetadata != null) {
			if (this.arrayTypes == null) this.arrayTypes = new IdentityHashMap<>();
			this.arrayTypes.put(extraction.values, extraction.typeMetadata);
		}
		return extraction;
	}

	void rememberSqlArrayExtraction(Array array, @Nullable Object values, @Nullable Throwable failure) {
		Object metadata = this.arrayTypes == null ? null : this.arrayTypes.get(values);
		rememberSqlArrayExtraction(array, new SqlArrayExtraction(values, metadata, failure));
	}

	private void rememberSqlArrayExtraction(Array array, SqlArrayExtraction extraction) {
		if (this.parent != null) {
			this.parent.rememberSqlArrayExtraction(array, extraction);
			return;
		}
		if (this.sqlArrayExtractions == null) this.sqlArrayExtractions = new IdentityHashMap<>();
		this.sqlArrayExtractions.put(array, extraction);
	}

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
		registerContainerType(value, typeName);
	}

	/** SQL declarations for STRUCT/MAP values and their nested containers, scoped to one row. */
	void registerContainerType(Object value, @Nullable String typeName) {
		if (typeName == null) return;
		if (this.containerTypeNames == null) this.containerTypeNames = new IdentityHashMap<>();
		this.containerTypeNames.put(value, typeName);
	}

	@Nullable String structType(Object value) {
		return containerType(value);
	}

	@Nullable String containerType(Object value) {
		return this.containerTypeNames == null ? null : this.containerTypeNames.get(value);
	}

	void copyStructType(Object original, Object converted) {
		if (converted != original) registerStructType(converted, structType(original));
	}

	static boolean requiresMaterialization(@Nullable Object raw, Class<?> target, int jdbcType) {
		return (target == byte[].class && (raw instanceof Blob || jdbcType == Types.BLOB))
				|| (target == String.class && (raw instanceof Clob || jdbcType == Types.CLOB || jdbcType == Types.NCLOB));
	}

	MappingResources(@Nullable Long maxBytes, @Nullable Long maxCharacters) {
		this(maxBytes, maxCharacters, false);
	}

	MappingResources(@Nullable Long maxBytes, @Nullable Long maxCharacters, boolean detachLobValues) {
		this(maxBytes, maxCharacters, detachLobValues, null);
	}

	private MappingResources(@Nullable Long maxBytes, @Nullable Long maxCharacters, boolean detachLobValues, @Nullable MappingResources parent) {
		this.parent = parent;
		this.detachLobValues = detachLobValues;
		// Even an explicitly unbounded Java value must fit in its array/string representation.
		this.maxBytes = Math.min(maxBytes == null ? Long.MAX_VALUE : maxBytes, Integer.MAX_VALUE - 8L);
		this.maxCharacters = Math.min(maxCharacters == null ? Long.MAX_VALUE : maxCharacters, Integer.MAX_VALUE - 8L);
	}

	void borrow(@Nullable Object value) {
		if (!(value instanceof Blob || value instanceof Clob || value instanceof SQLXML || value instanceof Struct || value instanceof Array
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
		boolean character = target == String.class && (raw instanceof Clob || raw instanceof SQLXML
				|| jdbcType == Types.CLOB || jdbcType == Types.NCLOB || jdbcType == Types.SQLXML);
		if (!binary && !character)
			return raw;
		long byteLimit = bounded ? this.maxBytes : Integer.MAX_VALUE - 8L;
		long characterLimit = bounded ? this.maxCharacters : Integer.MAX_VALUE - 8L;
		// SQLXML can permit only one read. Nested mapping in an opted-in row shares successful
		// XML reads with that active row, while ownership stays with the scope that acquired it.
		MappingResources memoScope = this;
		if (this.detachLobValues && raw instanceof SQLXML)
			while (memoScope.parent != null) memoScope = memoScope.parent;
		if (memoScope.materialized != null && memoScope.materialized.containsKey(raw)) {
			Object existing = memoScope.materialized.get(raw);
			// Native/dialect results may have been materialized without a typed-target limit.
			if (binary && existing instanceof byte[] bytes) checkLength(bytes.length, byteLimit, location, "byte[]", "bytes");
			if (character && existing instanceof String string) checkLength(string.length(), characterLimit, location, "String", "UTF-16 code units");
			return existing;
		}

		Object value = raw;
		try {
			if (raw instanceof Blob blob && binary) {
				own(blob);
				InputStream input = blob.getBinaryStream();
				Throwable primary = null;
				try {
					value = readBytes(input, location, byteLimit);
				} catch (IOException | RuntimeException | Error failure) {
					primary = failure;
					throw failure;
				} finally {
					closeStream(input, primary);
				}
			} else if (raw instanceof Clob clob && character) {
				own(clob);
				Reader reader = clob.getCharacterStream();
				Throwable primary = null;
				try {
					value = readCharacters(reader, location, characterLimit);
				} catch (IOException | RuntimeException | Error failure) {
					primary = failure;
					throw failure;
				} finally {
					closeStream(reader, primary);
				}
			} else if (raw instanceof SQLXML xml && character) {
				own(xml);
				value = readXml(xml, location, characterLimit);
			} else if (raw instanceof byte[] bytes && binary) {
				checkLength(bytes.length, byteLimit, location, "byte[]", "bytes");
			} else if (raw instanceof String string && character) {
				checkLength(string.length(), characterLimit, location, "String", "UTF-16 code units");
			}
		} catch (SQLException | IOException failure) {
			throw DatabaseException.wrapSafely("Unable to materialize LOB at " + location + " as " + target.getTypeName(),
					failure, DatabaseDialect.forExceptionCause(failure));
		}
		if (memoScope.materialized == null) memoScope.materialized = new IdentityHashMap<>();
		memoScope.materialized.put(raw, value);
		return value;
	}

	private @Nullable String readXml(SQLXML xml, String location, long limit) throws SQLException, IOException {
		Reader reader;
		try {
			reader = xml.getCharacterStream();
		} catch (SQLException | UnsupportedOperationException | AbstractMethodError unsupported) {
			if (!JdbcFallbacks.isExecutionFallbackSafe(unsupported)) throw unsupported;
			String value = xml.getString();
			if (value != null) checkLength(value.length(), limit, location, "String", "UTF-16 code units");
			return value;
		}
		Throwable primary = null;
		try {
			return readCharacters(reader, location, limit);
		} catch (IOException | RuntimeException | Error failure) {
			primary = failure;
			throw failure;
		} finally {
			closeStream(reader, primary);
		}
	}

	/** Driver streams can rethrow their read failure from close; never self-suppress it. */
	private static void closeStream(@Nullable Closeable stream, @Nullable Throwable primary) throws IOException {
		if (stream == null) return;
		try {
			stream.close();
		} catch (IOException | RuntimeException | Error failure) {
			if (primary == null) throw failure;
			if (primary != failure) primary.addSuppressed(failure);
		}
	}

	/** Native Map/Object targets keep dialect normalization without changing the typed LOB policy. */
	@Nullable Object nativeValue(@Nullable Object raw, DatabaseDialect dialect) {
		return nativeValue(raw, dialect, Types.OTHER, "native result value");
	}

	@Nullable Object nativeValue(@Nullable Object raw, DatabaseDialect dialect, int jdbcType, String location) {
		if (raw == null) return null;
		if (this.detachLobValues) {
			Object value = dialect instanceof DuckDbDialect duck ? duck.unwrapForTypedMapping(raw, this) : dialect.unwrapResultSetValue(raw);
			return detachLobs(value, jdbcType, location);
		}
		borrow(raw);
		if (!(dialect instanceof DuckDbDialect duck)) return dialect.unwrapResultSetValue(raw);
		return nativeDuckValue(duck.unwrapForTypedMapping(raw));
	}

	boolean detachesLobValues() { return this.detachLobValues; }

	/** Copies native Java containers while preserving aliases/cycles and detaching every visible LOB. */
	@Nullable Object detachLobs(@Nullable Object value, int jdbcType, String location) {
		if (value == null) return null;
		if (value instanceof Blob)
			return materialize(value, byte[].class, jdbcType, location);
		if (value instanceof Clob || value instanceof SQLXML)
			return materialize(value, String.class, jdbcType, location);
		if (jdbcType == Types.BLOB) {
			if (!(value instanceof byte[])) throw new DatabaseException("Driver exposed an unsupported binary LOB representation at " + location);
			return materialize(value, byte[].class, jdbcType, location);
		}
		if (jdbcType == Types.CLOB || jdbcType == Types.NCLOB || jdbcType == Types.SQLXML) {
			if (!(value instanceof String)) throw new DatabaseException("Driver exposed an unsupported character LOB representation at " + location);
			return materialize(value, String.class, jdbcType, location);
		}
		if (!(value instanceof Object[] || value instanceof Collection<?> || value instanceof Map<?, ?>)) return value;
		if (this.detachedContainers == null) this.detachedContainers = new IdentityHashMap<>();
		if (this.detachedContainers.containsKey(value)) {
			// An aliased driver array can acquire more specific base metadata after its first extraction.
			// Recheck known LOB bounds even when its detached container is already memoized.
			int elementJdbcType = arrayType(value);
			if (elementJdbcType == Types.BLOB || elementJdbcType == Types.CLOB || elementJdbcType == Types.NCLOB || elementJdbcType == Types.SQLXML) {
				if (value instanceof Object[] elements)
					for (Object element : elements) detachLobs(element, elementJdbcType, location + " element");
				else if (value instanceof Collection<?> elements)
					for (Object element : elements) detachLobs(element, elementJdbcType, location + " element");
			}
			return this.detachedContainers.get(value);
		}
		if (value instanceof Object[] elements) {
			// Native scalar/primitive arrays retain their class; LOB/container elements may change type.
			Class<?> leaf = value.getClass().getComponentType();
			while (leaf.isArray()) leaf = leaf.getComponentType();
			boolean stable = leaf.isPrimitive() || (Modifier.isFinal(leaf.getModifiers())
					&& !Blob.class.isAssignableFrom(leaf) && !Clob.class.isAssignableFrom(leaf)
					&& !SQLXML.class.isAssignableFrom(leaf) && !Map.class.isAssignableFrom(leaf)
					&& !Collection.class.isAssignableFrom(leaf));
			Object[] result = stable ? elements.clone() : new Object[elements.length];
			rememberDetachedContainer(value, result);
			int elementJdbcType = arrayType(value);
			for (int i = 0; i < elements.length; i++) result[i] = detachLobs(elements[i], elementJdbcType, location + " element " + i);
			return result;
		}
		if (value instanceof Map<?, ?> entries) {
			Map<Object, Object> result = new LinkedHashMap<>(entries.size());
			rememberDetachedContainer(value, result);
			for (Map.Entry<?, ?> entry : entries.entrySet()) {
				Object key = detachLobs(entry.getKey(), Types.OTHER, location + " map key");
				Object mapped = detachLobs(entry.getValue(), Types.OTHER, location + " map value");
				if (result.containsKey(key)) throw new DatabaseException("Native MAP contains duplicate keys after LOB detachment");
				result.put(key, mapped);
			}
			return result;
		}
		Collection<?> elements = (Collection<?>) value;
		Collection<Object> result = value instanceof Set<?> ? new LinkedHashSet<>(elements.size()) : new ArrayList<>(elements.size());
		rememberDetachedContainer(value, result);
		int index = 0;
		int elementJdbcType = arrayType(value);
		for (Object element : elements) {
			Object mapped = detachLobs(element, elementJdbcType, location + " element " + index++);
			if (!result.add(mapped)) throw new DatabaseException("Native SET contains duplicate elements after LOB detachment");
		}
		return result;
	}

	private void rememberDetachedContainer(Object original, Object detached) {
		this.detachedContainers.put(original, detached);
		this.detachedContainers.put(detached, detached);
		copyArrayType(original, detached);
		copyStructType(original, detached);
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

	boolean isBorrowed(Object resource) {
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
		close(null);
	}

	/** Retains a mapping failure while releasing every owned LOB, including reused driver failures. */
	void close(@Nullable Throwable primary) throws SQLException {
		Throwable cleanupFailure = null;
		try {
			if (this.owned != null) {
				for (Object resource : this.owned) {
					try {
						if (resource instanceof Blob blob) blob.free();
						else if (resource instanceof Clob clob) clob.free();
						else if (resource instanceof SQLXML xml) xml.free();
					} catch (SQLException | RuntimeException | Error failure) {
						if (failure == primary) continue;
						if (cleanupFailure == null) cleanupFailure = failure;
						else if (failure != cleanupFailure) cleanupFailure.addSuppressed(failure);
					}
				}
			}
		} finally {
			// Do not retain JDBC handles, detached values, user classes, or large row containers.
			this.owned = null;
			this.borrowed = null;
			this.materialized = null;
			this.detachedContainers = null;
			this.arrayTypes = null;
			this.sqlArrayExtractions = null;
			this.containerTypeNames = null;
			this.opaqueBorrow = false;
			this.inUse = false;
		}
		if (primary != null) {
			if (cleanupFailure != null) primary.addSuppressed(cleanupFailure);
			return;
		}
		if (cleanupFailure instanceof SQLException sql) throw sql;
		if (cleanupFailure instanceof RuntimeException runtime) throw runtime;
		if (cleanupFailure instanceof Error error) throw error;
	}
}
