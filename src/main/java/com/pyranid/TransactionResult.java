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
 * Indicates the transaction result reported to post-transaction operations.
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
	 * The transaction was rolled back.
	 * <p>
	 * This is the result when rollback completes successfully before commit is attempted, or when the physical commit reports
	 * a recognized serialization failure and Pyranid's follow-up rollback completes successfully.
	 */
	ROLLED_BACK,
	/**
	 * Pyranid cannot prove the final database outcome because commit failed without a recognized serialization failure followed
	 * by successful rollback, or because rollback failed.
	 * <p>
	 * For example, the database may have committed successfully but the client may not have received the acknowledgement, or
	 * rollback may have failed before the database confirmed that pending work was discarded.
	 *
	 * @since 4.2.0
	 */
	IN_DOUBT
}
