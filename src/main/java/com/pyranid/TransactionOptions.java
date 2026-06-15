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

import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Options for a Pyranid-managed transaction.
 * <p>
 * Use this when transaction behavior differs from the defaults. For example:
 * <pre>{@code
 * database.transaction(
 *   TransactionOptions.withIsolation(TransactionIsolation.REPEATABLE_READ)
 *     .readOnly(true)
 *     .build(),
 *   () -> {
 *     // transactional work
 *   });
 * }</pre>
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 4.2.0
 */
@ThreadSafe
public final class TransactionOptions {
	@NonNull
	static final TransactionOptions DEFAULT;

	static {
		DEFAULT = new TransactionOptions(TransactionIsolation.DEFAULT, null);
	}

	@NonNull
	private final TransactionIsolation isolation;
	@Nullable
	private final Boolean readOnly;

	private TransactionOptions(@NonNull TransactionIsolation isolation,
														 @Nullable Boolean readOnly) {
		requireNonNull(isolation);

		this.isolation = isolation;
		this.readOnly = readOnly;
	}

	/**
	 * Starts building transaction options with a transaction isolation level.
	 *
	 * @param isolation the desired database transaction isolation level
	 * @return a transaction options builder
	 */
	@NonNull
	public static Builder withIsolation(@NonNull TransactionIsolation isolation) {
		return new Builder().isolation(isolation);
	}

	/**
	 * Starts building transaction options with a read-only setting.
	 * <p>
	 * {@code true} requests a read-only transaction, {@code false} requests a read-write transaction, and {@code null}
	 * leaves the connection's read-only state unchanged.
	 *
	 * @param readOnly read-only setting to apply
	 * @return a transaction options builder
	 */
	@NonNull
	public static Builder withReadOnly(@Nullable Boolean readOnly) {
		return new Builder().readOnly(readOnly);
	}

	/**
	 * Gets the transaction isolation level.
	 *
	 * @return transaction isolation level
	 */
	@NonNull
	public TransactionIsolation getIsolation() {
		return this.isolation;
	}

	/**
	 * Gets the read-only setting to apply, if any.
	 * <p>
	 * {@code true} requests a read-only transaction, {@code false} requests a read-write transaction, and empty leaves the
	 * connection's read-only state unchanged.
	 *
	 * @return read-only setting to apply
	 */
	@NonNull
	public Optional<Boolean> getReadOnly() {
		return Optional.ofNullable(this.readOnly);
	}

	@Override
	public boolean equals(Object object) {
		if (this == object)
			return true;

		if (!(object instanceof TransactionOptions))
			return false;

		TransactionOptions transactionOptions = (TransactionOptions) object;
		return getIsolation() == transactionOptions.getIsolation()
				&& Objects.equals(getReadOnly(), transactionOptions.getReadOnly());
	}

	@Override
	public int hashCode() {
		return Objects.hash(getIsolation(), getReadOnly());
	}

	@Override
	@NonNull
	public String toString() {
		return String.format("%s{isolation=%s, readOnly=%s}",
				getClass().getSimpleName(), getIsolation().name(), getReadOnly().map(String::valueOf).orElse("unchanged"));
	}

	/**
	 * Builder used to construct {@link TransactionOptions}.
	 * <p>
	 * This class is intended for use by a single thread.
	 *
	 * @author <a href="https://www.revetkn.com">Mark Allen</a>
	 * @since 4.2.0
	 */
	@NotThreadSafe
	public static final class Builder {
		@NonNull
		private TransactionIsolation isolation;
		@Nullable
		private Boolean readOnly;

		private Builder() {
			this.isolation = TransactionIsolation.DEFAULT;
			this.readOnly = null;
		}

		/**
		 * Configures the transaction isolation level.
		 *
		 * @param isolation the desired database transaction isolation level
		 * @return this builder, for chaining
		 */
		@NonNull
		public Builder isolation(@NonNull TransactionIsolation isolation) {
			requireNonNull(isolation);
			this.isolation = isolation;
			return this;
		}

		/**
		 * Configures the transaction read-only setting.
		 * <p>
		 * {@code true} requests a read-only transaction, {@code false} requests a read-write transaction, and {@code null}
		 * leaves the connection's read-only state unchanged.
		 *
		 * @param readOnly read-only setting to apply
		 * @return this builder, for chaining
		 */
		@NonNull
		public Builder readOnly(@Nullable Boolean readOnly) {
			this.readOnly = readOnly;
			return this;
		}

		/**
		 * Builds transaction options.
		 *
		 * @return transaction options
		 */
		@NonNull
		public TransactionOptions build() {
			return new TransactionOptions(this.isolation, this.readOnly);
		}
	}
}
