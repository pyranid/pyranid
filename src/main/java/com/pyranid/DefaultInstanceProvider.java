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

import static java.lang.String.format;

/**
 * Basic implementation of {@link InstanceProvider} which uses {@link Class#newInstance()}.
 * 
 * @author <a href="https://www.revetware.com">Mark Allen</a>
 * @since 1.0.0
 */
public class DefaultInstanceProvider implements InstanceProvider {
  @Override
  public <T> T provide(Class<T> instanceClass) {
    try {
      return instanceClass.newInstance();
    } catch (Exception e) {
      throw new RuntimeException(format(
        "Unable to create an instance of %s. Please verify that %s has a public no-argument constructor",
        instanceClass, instanceClass.getSimpleName()), e);
    }
  }
}