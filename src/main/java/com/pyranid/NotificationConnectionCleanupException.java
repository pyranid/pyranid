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

/** Marks connection disposal failures without changing the original driver's exception graph. */
final class NotificationConnectionCleanupException extends DatabaseException {
	NotificationConnectionCleanupException(@NonNull String message, @NonNull Throwable cause, @NonNull DatabaseDialect dialect) {
		this(message, cause, dialect, true);
	}

	NotificationConnectionCleanupException(@NonNull String message, @NonNull Throwable cause, @NonNull DatabaseDialect dialect,
			boolean inspectCause) {
		super(message, cause, dialect, null, inspectCause);
	}
}
