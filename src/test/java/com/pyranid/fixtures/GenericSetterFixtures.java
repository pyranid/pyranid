/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid.fixtures;

import com.pyranid.DatabaseColumn;

/** javac creates a visibility bridge in the public child, but the useful generic metadata belongs to its hidden parent. */
public final class GenericSetterFixtures {
	private GenericSetterFixtures() {}
	static class HiddenEntity<T> {
		@DatabaseColumn("inherited_identifier")
		private T id;
		public void setId(T id) { this.id = id; }
		public T getId() { return this.id; }
	}
	public static class HiddenLongEntity extends HiddenEntity<Long> {
		public HiddenLongEntity() {}
	}
	public static class PrivateOverloadEntity extends HiddenEntity<Long> {
		public PrivateOverloadEntity() {}
		private void setId(String ignored) {}
	}
	public static class StaticOverloadEntity extends HiddenEntity<Long> {
		public StaticOverloadEntity() {}
		public static void setId(String ignored) {}
	}
}
