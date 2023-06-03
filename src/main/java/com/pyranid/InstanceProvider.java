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
 * Contract for a factory that creates instances given a type.
 * <p>
 * Useful for resultset mapping, where each row in the resultset might require a new instance.
 * <p>
 * Implementors are suggested to employ application-specific strategies, such as having a DI container handle instance
 * creation.
 * 
 * @author <a href="https://www.revetware.com">Mark Allen</a>
 * @since 1.0.0
 */
public interface InstanceProvider {
  /**
   * Provides an instance of the given {@code instanceClass}.
   * <p>
   * Whether the instance is new every time or shared/reused is implementation-dependent.
   * 
   * @param <T>
   *          instance type token
   * @param instanceClass
   *          the type of instance to create
   * @return an instance of the given {@code instanceClass}
   */
  <T> T provide(Class<T> instanceClass);
}