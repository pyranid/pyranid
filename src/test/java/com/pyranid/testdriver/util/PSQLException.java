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

package com.pyranid.testdriver.util;

import java.sql.SQLException;

/**
 * Stands in for a PostgreSQL JDBC driver fork's {@code PSQLException}, including the
 * {@code getServerErrorMessage()} accessor Pyranid reads rich diagnostics from.
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 4.7.0
 */
public class PSQLException extends SQLException {
	private final ServerErrorMessage serverErrorMessage;

	public PSQLException(String reason,
											 String sqlState,
											 ServerErrorMessage serverErrorMessage) {
		super(reason, sqlState);
		this.serverErrorMessage = serverErrorMessage;
	}

	public ServerErrorMessage getServerErrorMessage() {
		return this.serverErrorMessage;
	}
}
