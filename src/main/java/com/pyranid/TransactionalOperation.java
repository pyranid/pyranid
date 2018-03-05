/*
 * Copyright 2015-2018 Transmogrify LLC.
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
 * Represents a transactional operation.
 * <p>
 * See {@link ReturningTransactionalOperation} for a variant which supports returning a value.
 * 
 * @author <a href="http://revetkn.com">Mark Allen</a>
 * @since 1.0.0
 */
@FunctionalInterface
public interface TransactionalOperation {
  /**
   * Executes a transactional operation.
   * 
   * @throws Throwable
   *           if an error occurs while executing the transactional operation
   */
  void perform() throws Throwable;
}