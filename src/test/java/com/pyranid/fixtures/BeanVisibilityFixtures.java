/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.pyranid.fixtures;

import com.pyranid.DatabaseColumn;
import java.util.List;
import java.util.UUID;

/** Public beans outside the mapper's package, including javac visibility and erasure bridges. */
public final class BeanVisibilityFixtures {
	private BeanVisibilityFixtures() {}

	static class HiddenParent {
		@DatabaseColumn({"stored_name", "name_alias"})
		private String inherited = "initial";
		@DatabaseColumn("stored_ids")
		private List<UUID> identifiers;
		public void setInherited(String inherited) { this.inherited = inherited; }
		public String getInherited() { return this.inherited; }
		public void setIdentifiers(List<UUID> identifiers) { this.identifiers = identifiers; }
		public List<UUID> getIdentifiers() { return this.identifiers; }
	}

	static class HiddenMiddle extends HiddenParent {}

	public static class InheritedBean extends HiddenMiddle {
		private Integer visible;
		public InheritedBean() {}
		public void setVisible(Integer visible) { this.visible = visible; }
		public Integer getVisible() { return this.visible; }
	}

	public static class ExplicitOverrideBean extends HiddenParent {
		public ExplicitOverrideBean() {}
		@Override public void setInherited(String inherited) { super.setInherited(inherited); }
	}

	public static class PublicParent {
		private String inherited;
		public void setInherited(String inherited) { this.inherited = inherited; }
		public String getInherited() { return this.inherited; }
	}

	public static class PublicParentBean extends PublicParent {
		public PublicParentBean() {}
	}

	static class GenericParent<T> {
		private T value;
		public void setValue(T value) { this.value = value; }
		public T getValue() { return this.value; }
	}

	public static class GenericStringBean extends GenericParent<String> {
		public GenericStringBean() {}
		@Override public void setValue(String value) { super.setValue(value); }
		@Override public String getValue() { return super.getValue(); }
	}

	static class HiddenObjectParent {
		private Object choice;
		public void setChoice(Object choice) { this.choice = choice; }
		public Object getChoice() { return this.choice; }
	}

	public static class OverloadedBean extends HiddenObjectParent {
		public OverloadedBean() {}
		public void setChoice(String choice) { super.setChoice("typed:" + choice); }
	}
}
