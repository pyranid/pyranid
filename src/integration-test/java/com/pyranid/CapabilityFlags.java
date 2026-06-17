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
 * Feature gates used by portable integration tests.
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 4.3.0
 */
record CapabilityFlags(boolean supportsReadOnlyTransactions,
											 boolean supportsTransactionIsolationOptions,
											 boolean supportsTemporalRoundTrip,
											 boolean supportsServerSideStreaming,
											 boolean supportsSqlArrays,
											 boolean supportsNativeJson,
											 boolean supportsReturningClause,
											 boolean supportsOutputClause,
											 boolean supportsNativeUuid,
											 boolean hasNativeBoolean,
											 boolean reportsTimestampWithTimeZoneTypeName) {
	static final CapabilityFlags DEFAULT = builder().build();

	static Builder builder() {
		return new Builder();
	}

	static final class Builder {
		private boolean supportsReadOnlyTransactions;
		private boolean supportsTransactionIsolationOptions;
		private boolean supportsTemporalRoundTrip;
		private boolean supportsServerSideStreaming;
		private boolean supportsSqlArrays;
		private boolean supportsNativeJson;
		private boolean supportsReturningClause;
		private boolean supportsOutputClause;
		private boolean supportsNativeUuid;
		private boolean hasNativeBoolean;
		private boolean reportsTimestampWithTimeZoneTypeName;

		private Builder() {
			this.supportsReadOnlyTransactions = true;
			this.supportsTransactionIsolationOptions = true;
			this.supportsTemporalRoundTrip = true;
			this.supportsServerSideStreaming = false;
			this.supportsSqlArrays = false;
			this.supportsNativeJson = false;
			this.supportsReturningClause = false;
			this.supportsOutputClause = false;
			this.supportsNativeUuid = false;
			this.hasNativeBoolean = true;
			this.reportsTimestampWithTimeZoneTypeName = false;
		}

		Builder supportsReadOnlyTransactions(boolean supportsReadOnlyTransactions) {
			this.supportsReadOnlyTransactions = supportsReadOnlyTransactions;
			return this;
		}

		Builder supportsTransactionIsolationOptions(boolean supportsTransactionIsolationOptions) {
			this.supportsTransactionIsolationOptions = supportsTransactionIsolationOptions;
			return this;
		}

		Builder supportsTemporalRoundTrip(boolean supportsTemporalRoundTrip) {
			this.supportsTemporalRoundTrip = supportsTemporalRoundTrip;
			return this;
		}

		Builder supportsServerSideStreaming(boolean supportsServerSideStreaming) {
			this.supportsServerSideStreaming = supportsServerSideStreaming;
			return this;
		}

		Builder supportsSqlArrays(boolean supportsSqlArrays) {
			this.supportsSqlArrays = supportsSqlArrays;
			return this;
		}

		Builder supportsNativeJson(boolean supportsNativeJson) {
			this.supportsNativeJson = supportsNativeJson;
			return this;
		}

		Builder supportsReturningClause(boolean supportsReturningClause) {
			this.supportsReturningClause = supportsReturningClause;
			return this;
		}

		Builder supportsOutputClause(boolean supportsOutputClause) {
			this.supportsOutputClause = supportsOutputClause;
			return this;
		}

		Builder supportsNativeUuid(boolean supportsNativeUuid) {
			this.supportsNativeUuid = supportsNativeUuid;
			return this;
		}

		Builder hasNativeBoolean(boolean hasNativeBoolean) {
			this.hasNativeBoolean = hasNativeBoolean;
			return this;
		}

		Builder reportsTimestampWithTimeZoneTypeName(boolean reportsTimestampWithTimeZoneTypeName) {
			this.reportsTimestampWithTimeZoneTypeName = reportsTimestampWithTimeZoneTypeName;
			return this;
		}

		CapabilityFlags build() {
			return new CapabilityFlags(
					this.supportsReadOnlyTransactions,
					this.supportsTransactionIsolationOptions,
					this.supportsTemporalRoundTrip,
					this.supportsServerSideStreaming,
					this.supportsSqlArrays,
					this.supportsNativeJson,
					this.supportsReturningClause,
					this.supportsOutputClause,
					this.supportsNativeUuid,
					this.hasNativeBoolean,
					this.reportsTimestampWithTimeZoneTypeName);
		}
	}
}
