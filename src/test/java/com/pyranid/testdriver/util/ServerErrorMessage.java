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

/**
 * Stands in for a PostgreSQL JDBC driver fork's {@code ServerErrorMessage}.
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 4.7.0
 */
public class ServerErrorMessage {
	public String getSQLState() { return "23505"; }
	public String getConstraint() { return "employee_email_key"; }
	public String getTable() { return "employee"; }
	public String getColumn() { return "email"; }
	public String getDetail() { return "Key (email)=(a@b.c) already exists."; }
	public String getMessage() { return "duplicate key value violates unique constraint"; }
	public String getDatatype() { return null; }
	public String getFile() { return "nbtinsert.c"; }
	public String getHint() { return null; }
	public String getInternalQuery() { return null; }
	public String getRoutine() { return "_bt_check_unique"; }
	public String getSchema() { return "public"; }
	public String getSeverity() { return "ERROR"; }
	public String getWhere() { return null; }
	public int getInternalPosition() { return 0; }
	public int getLine() { return 434; }
	public int getPosition() { return 0; }
}
