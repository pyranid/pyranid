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

import org.jspecify.annotations.Nullable;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Allows specification of alternate column names for resultset mapping.
 * <p>
 * Useful in situations where column names are ugly, inconsistent, or do not map well to camel-case Java property names.
 * <p>
 * For example:
 *
 * <pre>
 * class Example {
 *   &#064;DatabaseColumn({ &quot;systok&quot;, &quot;sys_tok&quot; })
 *   UUID systemToken;
 *
 *   UUID getSystemToken() {
 *     return systemToken;
 *   }
 *
 *   void setSystemToken(UUID systemToken) {
 *     this.systemToken = systemToken;
 *   }
 * }
 *
 * database.query(&quot;SELECT systok FROM example&quot;).fetchObject(Example.class);
 * </pre>
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 1.0.0
 */
@Target({ElementType.FIELD, ElementType.RECORD_COMPONENT})
@Retention(RetentionPolicy.RUNTIME)
public @interface DatabaseColumn {
	@Nullable
	String[] value();
}
