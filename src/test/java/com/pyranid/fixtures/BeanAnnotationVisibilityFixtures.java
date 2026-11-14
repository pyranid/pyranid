/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid.fixtures;

import com.pyranid.DatabaseColumn;
import java.util.List;

/** Exercises javac visibility bridges without adding java.desktop introspection. */
public final class BeanAnnotationVisibilityFixtures {
	private BeanAnnotationVisibilityFixtures() {}

	static class HiddenParent<T> {
		@DatabaseColumn("stored_x") private int xCoordinate;
		@DatabaseColumn("stored_ids") private List<T> identifiers;
		public int getXCoordinate() { return xCoordinate; }
		public void setXCoordinate(int value) { xCoordinate = value; }
		public List<T> getIdentifiers() { return identifiers; }
		public void setIdentifiers(List<T> values) { identifiers = values; }
	}

	public static class VisibleLongBean extends HiddenParent<Long> {
		public VisibleLongBean() {}
	}
}
