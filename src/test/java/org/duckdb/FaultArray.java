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

package org.duckdb;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/** Fault handle in the vendor namespace so native nested-ARRAY normalization selects its real branch. */
public final class FaultArray implements Array {
	private final Throwable readFailure;
	private final Throwable cleanupFailure;
	private final AtomicInteger reads;
	private final AtomicInteger frees;

	public FaultArray(Throwable readFailure, Throwable cleanupFailure, AtomicInteger reads, AtomicInteger frees) {
		this.readFailure = readFailure;
		this.cleanupFailure = cleanupFailure;
		this.reads = reads;
		this.frees = frees;
	}

	@Override public Object getArray() throws SQLException {
		this.reads.incrementAndGet();
		throwIfPresent(this.readFailure);
		return new Object[]{7};
	}

	@Override public void free() throws SQLException {
		this.frees.incrementAndGet();
		throwIfPresent(this.cleanupFailure);
	}

	private static void throwIfPresent(Throwable failure) throws SQLException {
		if (failure instanceof SQLException sql) throw sql;
		if (failure instanceof RuntimeException runtime) throw runtime;
		if (failure instanceof Error error) throw error;
	}

	@Override public int getBaseType() { return Types.INTEGER; }
	@Override public String getBaseTypeName() { return "INTEGER"; }
	@Override public Object getArray(Map<String, Class<?>> map) throws SQLException { return getArray(); }
	@Override public Object getArray(long index, int count) throws SQLException { return getArray(); }
	@Override public Object getArray(long index, int count, Map<String, Class<?>> map) throws SQLException { return getArray(); }
	@Override public ResultSet getResultSet() throws SQLException { throw new SQLFeatureNotSupportedException(); }
	@Override public ResultSet getResultSet(Map<String, Class<?>> map) throws SQLException { return getResultSet(); }
	@Override public ResultSet getResultSet(long index, int count) throws SQLException { return getResultSet(); }
	@Override public ResultSet getResultSet(long index, int count, Map<String, Class<?>> map) throws SQLException { return getResultSet(); }
}
