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

package com.pyranid.fixtures;

import com.pyranid.DatabaseColumn;

import java.util.List;

/** Bytecode-only fixtures loaded by name through a disposable child loader in MemoryLeakTests. */
public final class ReloadableMappingTypes {
	private ReloadableMappingTypes() {}

	public record RecordRow(Integer id) {}
	public record Element(Integer id) {}
	public record CollectionRow(List<Element> elements) {}

	public static final class BeanRow {
		@DatabaseColumn("identifier")
		private Integer id;

		public BeanRow() {}

		public Integer getId() {
			return this.id;
		}

		public void setId(Integer id) {
			this.id = id;
		}
	}
}
