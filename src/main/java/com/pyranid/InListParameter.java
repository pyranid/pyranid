/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2025 Revetware LLC.
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
import javax.annotation.concurrent.ThreadSafe;
import java.util.Optional;

/**
 * Encapsulates a parameter intended for SQL {@code IN} list expansion.
 * <p>
 * Standard instances may be constructed via {@link Parameters#inList(java.util.Collection)},
 * {@link Parameters#inList(Object[])}, or the primitive-array overloads.
 * <p>
 * Implementations should be threadsafe.
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 4.0.0
 */
@ThreadSafe
public interface InListParameter {
	/**
	 * Gets the elements to be expanded into {@code ?} placeholders.
	 *
	 * @return the elements for the {@code IN} list
	 */
	@Nonnull
	Optional<Object[]> getElements();
}
