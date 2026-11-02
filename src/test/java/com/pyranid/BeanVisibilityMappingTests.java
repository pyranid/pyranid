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
package com.pyranid;

import com.pyranid.fixtures.BeanVisibilityFixtures.ExplicitOverrideBean;
import com.pyranid.fixtures.BeanVisibilityFixtures.GenericStringBean;
import com.pyranid.fixtures.BeanVisibilityFixtures.InheritedBean;
import com.pyranid.fixtures.BeanVisibilityFixtures.OverloadedBean;
import com.pyranid.fixtures.BeanVisibilityFixtures.PublicParentBean;
import org.hsqldb.jdbc.JDBCDataSource;
import org.junit.jupiter.api.Test;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class BeanVisibilityMappingTests {
	@Test
	void visibilityBridgeMapsInheritedPropertiesAlongsideDeclaredProperties() throws Exception {
		assertTrue(InheritedBean.class.getMethod("setInherited", String.class).isBridge());
		for (boolean cached : new boolean[]{false, true}) {
			Database database = database(cached);
			for (int execution = 0; execution < 2; execution++) {
				InheritedBean bean = database.query("SELECT 'mapped' AS inherited, 7 AS visible FROM (VALUES (0))")
						.fetchObject(InheritedBean.class).orElseThrow();
				assertEquals("mapped", bean.getInherited());
				assertEquals(7, bean.getVisible());
			}
		}
	}

	@Test
	void projectionContainingOnlyInheritedPropertyIsRecognized() {
		for (boolean cached : new boolean[]{false, true}) {
			Database database = database(cached);
			InheritedBean bean = database.query("SELECT 'mapped' AS inherited FROM (VALUES (0))")
					.fetchObject(InheritedBean.class).orElseThrow();
			assertEquals("mapped", bean.getInherited());
			assertNull(bean.getVisible());
		}
	}

	@Test
	void hiddenAncestorAliasesAndParameterizedSetterTypesArePreserved() throws Exception {
		assertEquals(List.class, InheritedBean.class.getMethod("setIdentifiers", List.class).getGenericParameterTypes()[0]);
		UUID first = UUID.fromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");
		UUID second = UUID.fromString("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb");
		for (boolean cached : new boolean[]{false, true}) {
			Database database = database(cached);
			for (int execution = 0; execution < 2; execution++) {
				InheritedBean bean = database.query("""
						SELECT 'aliased' AS name_alias,
						       ARRAY['aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', 'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb'] AS stored_ids
						FROM (VALUES (0))
						""")
						.fetchObject(InheritedBean.class).orElseThrow();
				assertEquals("aliased", bean.getInherited());
				assertEquals(List.of(first, second), bean.getIdentifiers());
			}
		}
	}

	@Test
	void selectedNullClearsInheritedPropertyButOmittedColumnKeepsItsInitialValue() {
		for (boolean cached : new boolean[]{false, true}) {
			Database database = database(cached);
			InheritedBean selectedNull = database.query("SELECT CAST(NULL AS VARCHAR(20)) AS stored_name, 7 AS visible FROM (VALUES (0))")
					.fetchObject(InheritedBean.class).orElseThrow();
			assertNull(selectedNull.getInherited());
			assertEquals(7, selectedNull.getVisible());
			InheritedBean omitted = database.query("SELECT 7 AS visible FROM (VALUES (0))")
					.fetchObject(InheritedBean.class).orElseThrow();
			assertEquals("initial", omitted.getInherited());
		}
	}

	@Test
	void publicAncestorAndExplicitOverrideRemainUsable() {
		for (boolean cached : new boolean[]{false, true}) {
			Database database = database(cached);
			assertEquals("mapped", database.query("SELECT 'mapped' AS inherited FROM (VALUES (0))")
					.fetchObject(PublicParentBean.class).orElseThrow().getInherited());
			assertEquals("mapped", database.query("SELECT 'mapped' AS stored_name FROM (VALUES (0))")
					.fetchObject(ExplicitOverrideBean.class).orElseThrow().getInherited());
		}
	}

	@Test
	void erasedGenericBridgeDoesNotReplaceTheRealTypedSetter() throws Exception {
		assertTrue(GenericStringBean.class.getMethod("setValue", Object.class).isBridge());
		for (boolean cached : new boolean[]{false, true}) {
			CustomColumnMapper strings = new CustomColumnMapper() {
				@Override public Boolean appliesTo(TargetType target) { return target.matchesClass(String.class); }
				@Override public MappingResult map(StatementContext<?> context, java.sql.ResultSet resultSet, Object value,
						TargetType target, Integer index, String label, InstanceProvider provider) {
					return MappingResult.of("mapped:" + value);
				}
			};
			GenericStringBean bean = database(cached, List.of(strings)).query("SELECT 123 AS value FROM (VALUES (0))")
					.fetchObject(GenericStringBean.class).orElseThrow();
			assertEquals("mapped:123", bean.getValue(), "Custom mapping must see String rather than the erased Object type");
		}
	}

	@Test
	void realSubclassOverloadTakesPrecedenceOverAnInheritedVisibilityBridge() throws Exception {
		assertTrue(OverloadedBean.class.getMethod("setChoice", Object.class).isBridge());
		for (boolean cached : new boolean[]{false, true}) {
			OverloadedBean bean = database(cached).query("SELECT 'value' AS choice FROM (VALUES (0))")
					.fetchObject(OverloadedBean.class).orElseThrow();
			assertEquals("typed:value", bean.getChoice());
		}
	}

	private Database database(boolean cached) {
		return database(cached, List.of());
	}

	private Database database(boolean cached, List<CustomColumnMapper> customColumnMappers) {
		JDBCDataSource source = new JDBCDataSource();
		source.setUrl("jdbc:hsqldb:mem:bean_visibility");
		source.setUser("sa");
		return Database.with(source, DatabaseType.GENERIC)
				.resultSetMapper(ResultSetMapper.withPlanCachingEnabled(cached).customColumnMappers(customColumnMappers).build()).build();
	}
}
