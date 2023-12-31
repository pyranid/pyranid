/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2024 Revetware LLC.
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

/**
 * Contract for handling database statement log events.
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 1.0.0
 */
@FunctionalInterface
public interface StatementLogger {
	/**
	 * Performs a logging operation on the given {@code statementLog}.
	 * <p>
	 * Implementors might choose to no-op, write to stdout or a logging framework, send alerts about slow queries, and so
	 * on.
	 *
	 * @param statementLog The event to log
	 */
	void log(@Nonnull StatementLog statementLog);
}