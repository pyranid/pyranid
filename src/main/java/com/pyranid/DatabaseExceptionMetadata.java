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

import java.sql.SQLException;

/**
 * Database-specific metadata extracted from an exception cause.
 */
final class DatabaseExceptionMetadata {
	@Nullable
	final Integer errorCode;
	@Nullable
	final String sqlState;
	@Nullable
	final String column;
	@Nullable
	final String constraint;
	@Nullable
	final String datatype;
	@Nullable
	final String detail;
	@Nullable
	final String file;
	@Nullable
	final String hint;
	@Nullable
	final Integer internalPosition;
	@Nullable
	final String internalQuery;
	@Nullable
	final Integer line;
	@Nullable
	final String dbmsMessage;
	@Nullable
	final Integer position;
	@Nullable
	final String routine;
	@Nullable
	final String schema;
	@Nullable
	final String severity;
	@Nullable
	final String table;
	@Nullable
	final String where;

	private DatabaseExceptionMetadata(@NonNull Builder builder) {
		this.errorCode = builder.errorCode;
		this.sqlState = builder.sqlState;
		this.column = builder.column;
		this.constraint = builder.constraint;
		this.datatype = builder.datatype;
		this.detail = builder.detail;
		this.file = builder.file;
		this.hint = builder.hint;
		this.internalPosition = builder.internalPosition;
		this.internalQuery = builder.internalQuery;
		this.line = builder.line;
		this.dbmsMessage = builder.dbmsMessage;
		this.position = builder.position;
		this.routine = builder.routine;
		this.schema = builder.schema;
		this.severity = builder.severity;
		this.table = builder.table;
		this.where = builder.where;
	}

	@NonNull
	static DatabaseExceptionMetadata fromCause(@Nullable Throwable cause) {
		return builder(cause).build();
	}

	@NonNull
	static Builder builder(@Nullable Throwable cause) {
		Builder builder = new Builder();

		if (cause instanceof SQLException sqlException) {
			builder.errorCode(sqlException.getErrorCode());
			builder.sqlState(sqlException.getSQLState());
		}

		return builder;
	}

	static final class Builder {
		@Nullable
		private Integer errorCode;
		@Nullable
		private String sqlState;
		@Nullable
		private String column;
		@Nullable
		private String constraint;
		@Nullable
		private String datatype;
		@Nullable
		private String detail;
		@Nullable
		private String file;
		@Nullable
		private String hint;
		@Nullable
		private Integer internalPosition;
		@Nullable
		private String internalQuery;
		@Nullable
		private Integer line;
		@Nullable
		private String dbmsMessage;
		@Nullable
		private Integer position;
		@Nullable
		private String routine;
		@Nullable
		private String schema;
		@Nullable
		private String severity;
		@Nullable
		private String table;
		@Nullable
		private String where;

		@NonNull
		Builder errorCode(@Nullable Integer errorCode) {
			this.errorCode = errorCode;
			return this;
		}

		@NonNull
		Builder sqlState(@Nullable String sqlState) {
			this.sqlState = sqlState;
			return this;
		}

		@NonNull
		Builder column(@Nullable String column) {
			this.column = column;
			return this;
		}

		@NonNull
		Builder constraint(@Nullable String constraint) {
			this.constraint = constraint;
			return this;
		}

		@NonNull
		Builder datatype(@Nullable String datatype) {
			this.datatype = datatype;
			return this;
		}

		@NonNull
		Builder detail(@Nullable String detail) {
			this.detail = detail;
			return this;
		}

		@NonNull
		Builder file(@Nullable String file) {
			this.file = file;
			return this;
		}

		@NonNull
		Builder hint(@Nullable String hint) {
			this.hint = hint;
			return this;
		}

		@NonNull
		Builder internalPosition(@Nullable Integer internalPosition) {
			this.internalPosition = internalPosition;
			return this;
		}

		@NonNull
		Builder internalQuery(@Nullable String internalQuery) {
			this.internalQuery = internalQuery;
			return this;
		}

		@NonNull
		Builder line(@Nullable Integer line) {
			this.line = line;
			return this;
		}

		@NonNull
		Builder dbmsMessage(@Nullable String dbmsMessage) {
			this.dbmsMessage = dbmsMessage;
			return this;
		}

		@NonNull
		Builder position(@Nullable Integer position) {
			this.position = position;
			return this;
		}

		@NonNull
		Builder routine(@Nullable String routine) {
			this.routine = routine;
			return this;
		}

		@NonNull
		Builder schema(@Nullable String schema) {
			this.schema = schema;
			return this;
		}

		@NonNull
		Builder severity(@Nullable String severity) {
			this.severity = severity;
			return this;
		}

		@NonNull
		Builder table(@Nullable String table) {
			this.table = table;
			return this;
		}

		@NonNull
		Builder where(@Nullable String where) {
			this.where = where;
			return this;
		}

		@NonNull
		DatabaseExceptionMetadata build() {
			return new DatabaseExceptionMetadata(this);
		}
	}
}
