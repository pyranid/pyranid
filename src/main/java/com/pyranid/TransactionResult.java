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
 * Indicates the transaction result reported to post-transaction operations and completion observers.
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 2.0.0
 */
public enum TransactionResult {
	/**
	 * Commit completed successfully.
	 */
	COMMITTED,
	/**
	 * Pyranid can prove that no application transaction work committed.
	 * <p>
	 * This includes a successful rollback before commit, a successful follow-up rollback after the physical commit reports a
	 * recognized serialization failure, a successful rollback after a savepoint-scope ownership conflict refused the commit
	 * before it was issued, and a failed physical-transaction begin whose candidate connection Pyranid discarded before
	 * application transaction work could execute. Therefore, this result does not necessarily mean that Pyranid invoked the
	 * JDBC {@code rollback()} method.
	 */
	ROLLED_BACK,
	/**
	 * Pyranid cannot prove the final database outcome because the commit attempt failed without a recognized serialization
	 * failure followed by successful rollback, or because rollback failed after application transaction work could have
	 * executed. The commit attempt need not have reached the driver: once application transaction work could have executed,
	 * a failure anywhere on the commit path is reported here unless it is one of the provable cases named under
	 * {@link #ROLLED_BACK}.
	 * <p>
	 * For example, the database may have committed successfully but the client may not have received the acknowledgement, or
	 * rollback may have failed before the database confirmed that pending work was discarded.
	 * A rollback failure while cleaning up a failed physical-transaction begin is not {@code IN_DOUBT} when application
	 * transaction work could not have executed; Pyranid discards the candidate connection and reports {@link #ROLLED_BACK}.
	 * A savepoint-scope ownership conflict that refuses the commit before it is issued is not {@code IN_DOUBT} when the
	 * follow-up rollback succeeds; the database never received the commit, so Pyranid reports {@link #ROLLED_BACK}. The same
	 * conflict followed by a failed rollback remains {@code IN_DOUBT}.
	 *
	 * @since 4.2.0
	 */
	IN_DOUBT
}
