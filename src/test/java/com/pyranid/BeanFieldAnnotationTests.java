/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import com.pyranid.fixtures.BeanAnnotationVisibilityFixtures.VisibleLongBean;
import org.hsqldb.jdbc.JDBCDataSource;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class BeanFieldAnnotationTests {
	public static class NamedBean {
		@DatabaseColumn("stored_active") private boolean isActive;
		@DatabaseColumn("stored_x") private int xCoordinate;
		@DatabaseColumn("stored_url") private String URL;
		public boolean isActive() { return isActive; }
		public void setActive(boolean value) { isActive = value; }
		public int getXCoordinate() { return xCoordinate; }
		public void setXCoordinate(int value) { xCoordinate = value; }
		public String getURL() { return URL; }
		public void setURL(String value) { URL = value; }
	}
	public static class ExactIsNames {
		@DatabaseColumn("stored_ready") private Boolean isReady;
		@DatabaseColumn("stored_active") private boolean isactive;
		public Boolean getIsReady() { return isReady; }
		public void setIsReady(Boolean value) { isReady = value; }
		public boolean isIsactive() { return isactive; }
		public void setIsactive(boolean value) { isactive = value; }
	}
	public static class Parent {
		@DatabaseColumn("old_active") private boolean active;
		public void setActive(boolean value) { active = value; }
		public boolean getActive() { return active; }
	}
	public static class RenamedChild extends Parent {
		@DatabaseColumn("new_active") private boolean isActive;
	}
	public static class SameNameChild extends Parent {
		@DatabaseColumn("new_active") private boolean active;
	}
	public static class UnannotatedShadow extends Parent { private boolean active; }
	public static class PrimitiveIsParent {
		@DatabaseColumn("old_active") private boolean isActive;
		public void setActive(boolean value) { isActive = value; }
		public boolean getActive() { return isActive; }
	}
	public static class BoxedAnnotatedShadow extends PrimitiveIsParent {
		@DatabaseColumn("new_active") private Boolean isActive;
		public void setIsActive(Boolean value) { isActive = value; }
		public Boolean getIsActive() { return isActive; }
	}
	public static class ReadOnly {
		@DatabaseColumn("stored") private String value;
		public String getValue() { return value; }
	}
	public static class PartiallyWritable {
		private int id;
		@DatabaseColumn("stored_value") private String orphan;
		public void setId(int value) { id = value; }
		public int getId() { return id; }
		public String getOrphan() { return orphan; }
	}
	public static class UnannotatedReadOnly {
		private int id;
		private String orphan;
		public void setId(int value) { id = value; }
		public int getId() { return id; }
		public String getOrphan() { return orphan; }
	}
	public static class PrivateSetter {
		@DatabaseColumn("stored") private String value;
		private void setValue(String value) { this.value = value; }
	}
	public static class FluentSetter {
		@DatabaseColumn("stored") private String value;
		public FluentSetter setValue(String value) { this.value = value; return this; }
	}
	public static class NonBooleanIsPrefix {
		@DatabaseColumn("stored") private String isValue;
		public void setValue(String value) { isValue = value; }
	}
	public static class StaticField {
		@DatabaseColumn("stored") private static String value;
		public void setValue(String input) { value = input; }
	}
	public static class AmbiguousBoolean {
		@DatabaseColumn("stored") private boolean isActive;
		public void setActive(boolean value) { isActive = value; }
		public void setIsActive(boolean value) { isActive = value; }
	}
	public static class AmbiguousCapitalization {
		@DatabaseColumn("stored") private String xCoordinate;
		public void setxCoordinate(String value) { xCoordinate = value; }
		public void setXCoordinate(String value) { xCoordinate = value; }
	}
	public static class CollidingFields {
		@DatabaseColumn("first") private boolean active;
		@DatabaseColumn("second") private boolean isActive;
		public void setActive(boolean value) { active = value; isActive = value; }
	}
	public static class Fanout {
		@DatabaseColumn("shared") private int left;
		@DatabaseColumn("shared") private int right;
		public void setLeft(int value) { left = value; }
		public void setRight(int value) { right = value; }
	}
	public static class FinalTransient {
		@DatabaseColumn("stored_value") private final List<String> value = new ArrayList<>();
		@DatabaseColumn("stored_note") private transient String note;
		public void setValue(String input) { value.clear(); value.add(input); }
		public void setNote(String input) { note = input; }
	}
	public record AnnotatedRecord(@DatabaseColumn("stored") String xCoordinate) {}

	@Test
	void fieldAliasesFollowBeanSetterNamesRatherThanRawFieldSpelling() {
		for (boolean cached : new boolean[]{false, true}) {
			Database database = database(cached);
			for (int execution = 0; execution < 2; execution++) {
				NamedBean bean = database.query("SELECT TRUE AS stored_active, 7 AS stored_x, 'site' AS stored_url FROM (VALUES (0))")
						.fetchObject(NamedBean.class).orElseThrow();
				assertTrue(bean.isActive()); assertEquals(7, bean.getXCoordinate()); assertEquals("site", bean.getURL());
				NamedBean defaultNames = database.query("SELECT TRUE AS active, 8 AS \"XCoordinate\", 'default' AS \"URL\" FROM (VALUES (0))")
						.fetchObject(NamedBean.class).orElseThrow();
				assertTrue(defaultNames.isActive()); assertEquals(8, defaultNames.getXCoordinate()); assertEquals("default", defaultNames.getURL());
			}
		}
	}

	@Test
	void boxedBooleanAndNonLombokIsNamesRetainTheirExactProperties() {
		for (boolean cached : new boolean[]{false, true}) {
			ExactIsNames bean = database(cached).query("SELECT TRUE AS stored_ready, TRUE AS stored_active FROM (VALUES (0))")
					.fetchObject(ExactIsNames.class).orElseThrow();
			assertTrue(bean.getIsReady()); assertTrue(bean.isIsactive());
		}
	}

	@Test
	void inheritedAliasesWorkWithPublicVisibilityBridgesAndResolvedGenericSetters() throws Exception {
		assertTrue(VisibleLongBean.class.getMethod("setXCoordinate", int.class).isBridge());
		assertTrue(VisibleLongBean.class.getMethod("setIdentifiers", List.class).isBridge());
		for (boolean cached : new boolean[]{false, true}) {
			Database database = database(cached);
			for (int execution = 0; execution < 2; execution++) {
				VisibleLongBean bean = database.query("SELECT 7 AS stored_x, ARRAY[1,2] AS stored_ids FROM (VALUES (0))")
						.fetchObject(VisibleLongBean.class).orElseThrow();
				assertEquals(7, bean.getXCoordinate()); assertEquals(List.of(1L, 2L), bean.getIdentifiers());
			}
		}
	}

	@Test
	void nearestAnnotatedDeclarationWinsWhileUnannotatedShadowsKeepInheritedAliases() {
		for (boolean cached : new boolean[]{false, true}) {
			Database database = database(cached);
			for (Class<? extends Parent> type : List.of(RenamedChild.class, SameNameChild.class)) {
				assertTrue(database.query("SELECT TRUE AS new_active FROM (VALUES (0))").fetchObject(type).orElseThrow().getActive());
				assertFalse(database.query("SELECT FALSE AS active, TRUE AS old_active FROM (VALUES (0))").fetchObject(type).orElseThrow().getActive());
			}
			assertTrue(database.query("SELECT TRUE AS old_active FROM (VALUES (0))").fetchObject(UnannotatedShadow.class).orElseThrow().getActive());
			BoxedAnnotatedShadow boxed = database.query("SELECT TRUE AS new_active FROM (VALUES (0))").fetchObject(BoxedAnnotatedShadow.class).orElseThrow();
			assertTrue(boxed.getIsActive()); assertFalse(boxed.getActive());
		}
	}

	@Test
	void unmatchedAnnotationsFailEvenWhenTheirColumnsAreNotSelected() {
		for (boolean cached : new boolean[]{false, true}) {
			Database database = database(cached);
			for (Class<?> type : List.of(ReadOnly.class, PrivateSetter.class, FluentSetter.class, NonBooleanIsPrefix.class, StaticField.class)) {
				DatabaseException failure = assertThrows(DatabaseException.class, () -> database.query(
						"SELECT CAST(:value AS VARCHAR(64)) AS unrelated FROM (VALUES (0))")
						.bind("value", Parameters.secure("private-row-value-7451")).fetchObject(type));
				assertTrue(failure.getMessage().contains("@DatabaseColumn"));
				assertTrue(failure.getMessage().contains(type.getName()));
				assertFalse(failure.getMessage().contains("private-row-value-7451"));
			}
			String partialSql = "SELECT 7 AS id, CAST(:value AS VARCHAR(64)) AS unrelated FROM (VALUES (0))";
			DatabaseException partial = assertThrows(DatabaseException.class, () -> database.query(partialSql)
					.bind("value", Parameters.secure("private-row-value-7451")).fetchObject(PartiallyWritable.class));
			assertTrue(partial.getMessage().contains("@DatabaseColumn"));
			assertTrue(partial.getMessage().contains("orphan"));
			assertTrue(partial.getMessage().contains(PartiallyWritable.class.getName()));
			assertFalse(partial.getMessage().contains("private-row-value-7451"));
			UnannotatedReadOnly allowed = database.query(partialSql).bind("value", Parameters.secure("private-row-value-7451"))
					.fetchObject(UnannotatedReadOnly.class).orElseThrow();
			assertEquals(7, allowed.getId());
			assertNull(allowed.getOrphan(), "Ordinary unannotated read-only storage is still ignored");
		}
	}

	@Test
	void ambiguousFieldAssociationsAndCompetingFieldsFailDeterministically() {
		for (boolean cached : new boolean[]{false, true}) {
			Database database = database(cached);
			for (Class<?> type : List.of(AmbiguousBoolean.class, AmbiguousCapitalization.class, CollidingFields.class)) {
				DatabaseException failure = assertThrows(DatabaseException.class, () -> database.query("SELECT TRUE AS stored FROM (VALUES (0))").fetchObject(type));
				assertTrue(failure.getMessage().contains("@DatabaseColumn"));
				assertTrue(failure.getMessage().contains(type.getName()));
				if (type == CollidingFields.class) assertTrue(failure.getMessage().contains("both identify writable property 'active'"));
				else assertTrue(failure.getMessage().contains("ambiguously identifies writable properties"));
			}
		}
	}

	@Test
	void aliasesStillSupportFanoutFinalAndTransientStorageAndRecordComponents() {
		for (boolean cached : new boolean[]{false, true}) {
			Database database = database(cached);
			Fanout fanout = database.query("SELECT 7 AS shared FROM (VALUES (0))").fetchObject(Fanout.class).orElseThrow();
			assertEquals(7, fanout.left); assertEquals(7, fanout.right);
			FinalTransient bean = database.query("SELECT 'stored' AS stored_value, 'note' AS stored_note FROM (VALUES (0))")
					.fetchObject(FinalTransient.class).orElseThrow();
			assertEquals(List.of("stored"), bean.value); assertEquals("note", bean.note);
			assertEquals(new AnnotatedRecord("record"), database.query("SELECT 'record' AS stored FROM (VALUES (0))").fetchObject(AnnotatedRecord.class).orElseThrow());
			assertEquals(new AnnotatedRecord("default"), database.query("SELECT 'default' AS x_coordinate FROM (VALUES (0))")
					.fetchObject(AnnotatedRecord.class).orElseThrow());
		}
	}

	private static Database database(boolean cached) {
		JDBCDataSource source = new JDBCDataSource(); source.setUrl("jdbc:hsqldb:mem:field_annotations_" + UUID.randomUUID());
		source.setUser("sa"); source.setPassword("");
		return Database.with(source, DatabaseType.GENERIC).resultSetMapper(ResultSetMapper.withPlanCachingEnabled(cached).build()).build();
	}
}
