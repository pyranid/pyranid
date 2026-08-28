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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Per-leg override of the minimum number of inherited portable contract tests an integration leg must
 * execute (not skip). The default floor assumes at most two capability-based skips; a leg whose database
 * legitimately cannot run more of the portable contract declares its own floor here, with a comment
 * enumerating each capability-based skip, so other legs keep the stricter default.
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 4.7.0
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@interface PortableContractCoverageFloor {
	int value();
}
