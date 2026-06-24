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

import javax.annotation.concurrent.ThreadSafe;
import java.util.Optional;

/**
 * Wraps a bound parameter value so Pyranid can mask it in diagnostics while still binding the underlying value normally.
 * <p>
 * This is display-only: it does not change what is bound to the database. Pyranid's own diagnostic rendering reads
 * {@link #getMask()} directly and does not rely on an implementation's {@link Object#toString()} method.
 * <p>
 * Implementations should be threadsafe.
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 4.4.0
 */
@ThreadSafe
public interface SecureParameter {
	/**
	 * Gets the value to bind.
	 *
	 * @return the value to bind, or {@link Optional#empty()} if the value is {@code null}
	 */
	@NonNull
	Optional<Object> getValue();

	/**
	 * Gets the safe display value Pyranid should render in diagnostics.
	 *
	 * @return the safe display value
	 */
	@NonNull
	String getMask();
}
