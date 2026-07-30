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

/**
 * Represents an operation performed with a callback-scoped {@link NotificationSession}.
 * <p>
 * Pyranid invokes the operation synchronously, at most once, after all requested notification channels have been
 * registered. The session is valid only while {@link #perform(NotificationSession)} is active and must not be retained
 * or used from another thread.
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 4.6.0
 */
@FunctionalInterface
public interface NotificationSessionOperation {
	/**
	 * Performs work with the provided notification session.
	 *
	 * @param session callback-scoped notification session
	 * @throws Exception if the operation fails
	 * @since 4.6.0
	 */
	void perform(@NonNull NotificationSession session) throws Exception;
}
