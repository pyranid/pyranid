/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.hsqldb.jdbc.JDBCDataSource;
import org.junit.jupiter.api.Test;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class MappingExtensionRecipesTests {

	interface AfterMapping { void afterMapping(); }
	public static class Contact implements AfterMapping {
		private String name;
		private int calls;
		public Contact() {}
		public void setName(String name) { this.name = name; }
		public String getName() { return name; }
		@Override public void afterMapping() { name = name.strip(); calls++; }
	}
	public record ContactRecord(String name, String injected) {}
	private record PrivateContact(String name) {}
	public record ContactProjection(String name) {
		DomainContact toDomain() { return new DomainContact(name.strip()); }
	}
	private static final class DomainContact {
		final String name;
		DomainContact(String name) { this.name = name; }
	}

	static ResultSetMapper afterMappingMapper() {
		ResultSetMapper defaults = ResultSetMapper.withDefaultConfiguration();
		return new ResultSetMapper() {
			@Override public <T> Optional<T> map(StatementContext<T> context, ResultSet rows,
					Class<T> type, InstanceProvider provider) throws SQLException {
				Optional<T> mapped = defaults.map(context, rows, type, provider);
				mapped.ifPresent(value -> {
					if (value instanceof AfterMapping hook) hook.afterMapping();
				});
				return mapped;
			}
		};
	}

	@Test
	void beanHookRunsAfterSettersExactlyOnceForEachListAndStreamRow() {
		Database database = database(null);
		String sql = "SELECT name FROM (VALUES (' Ada '), (' Grace ')) AS names(name) ORDER BY name";
		for (boolean stream : new boolean[]{false, true}) {
			Query query = database.query(sql).resultSetMapper(afterMappingMapper());
			List<Contact> rows = stream ? query.fetchStream(Contact.class, values -> values.toList()) : query.fetchList(Contact.class);
			assertEquals(List.of("Ada", "Grace"), rows.stream().map(Contact::getName).toList());
			assertTrue(rows.stream().allMatch(row -> row.calls == 1));
		}
		assertTrue(database.query("SELECT name FROM (VALUES ('Ada')) AS names(name) WHERE 1=0")
				.resultSetMapper(afterMappingMapper()).fetchObject(Contact.class).isEmpty());
		assertTrue(database.query("VALUES (CAST(NULL AS VARCHAR(20)))").resultSetMapper(afterMappingMapper())
				.fetchObject(String.class).isEmpty());
	}

	@Test
	void hookFailureEscapingTheTransactionRollsBackEarlierWrites() {
		Database database = database(null);
		database.query("CREATE TABLE item (id INTEGER PRIMARY KEY)").execute();
		assertThrows(DatabaseException.class, () -> database.transaction(() -> {
			database.query("INSERT INTO item VALUES (1)").execute();
			database.query("SELECT CAST(NULL AS VARCHAR(20)) AS name").resultSetMapper(afterMappingMapper())
					.fetchObject(Contact.class);
		}));
		assertEquals(0L, database.query("SELECT COUNT(*) FROM item").fetchObject(Long.class).orElseThrow());
	}

	@Test
	void recordProviderInjectsAfterColumnConversionAndDoesNotModifySharedArguments() {
		AtomicInteger calls = new AtomicInteger();
		InstanceProvider provider = new InstanceProvider() {
			@Override public <T extends Record> T provideRecord(StatementContext<T> context, Class<T> type, Object... values) {
				if (type == ContactRecord.class) {
					assertEquals("Ada", values[0]);
					assertNull(values[1]);
					values = values.clone();
					values[1] = "application-service";
					calls.incrementAndGet();
				}
				return InstanceProvider.super.provideRecord(context, type, values);
			}
		};
		Database database = database(provider);
		assertEquals(new ContactRecord("Ada", "application-service"), database.query("SELECT name FROM (VALUES ('Ada')) AS names(name)")
				.fetchObject(ContactRecord.class).orElseThrow());
		assertEquals(1, calls.get());
	}

	@Test
	void applicationLookupCanConstructAnExplicitlyAllowedPrivateRecord() {
		Database defaults = database(null);
		assertThrows(DatabaseException.class, () -> defaults.query("SELECT 'Ada' AS name FROM (VALUES (0))")
				.fetchObject(PrivateContact.class));
		InstanceProvider provider = new InstanceProvider() {
			@Override public <T extends Record> T provideRecord(StatementContext<T> context, Class<T> type, Object... values) {
				if (type != PrivateContact.class) return InstanceProvider.super.provideRecord(context, type, values);
				try {
					Class<?>[] parameters = Arrays.stream(type.getRecordComponents()).map(component -> component.getType()).toArray(Class<?>[]::new);
					var lookup = MethodHandles.privateLookupIn(type, MethodHandles.lookup());
					return type.cast(lookup.findConstructor(type, MethodType.methodType(void.class, parameters)).invokeWithArguments(values));
				} catch (RuntimeException | Error failure) {
					throw failure;
				} catch (Throwable failure) {
					throw new IllegalStateException("Cannot construct " + type.getName(), failure);
				}
			}
		};
		Database database = database(provider);
		assertEquals(new PrivateContact("Ada"), database.query("SELECT 'Ada' AS name FROM (VALUES (0))")
				.fetchObject(PrivateContact.class).orElseThrow());
		assertEquals(new ContactProjection("Ada"), database.query("SELECT 'Ada' AS name FROM (VALUES (0))")
				.fetchObject(ContactProjection.class).orElseThrow());
	}

	@Test
	void immutableDomainObjectsUseARecordProjectionAndExplicitPostFetchConversion() {
		Database database = database(null);
		List<DomainContact> contacts = database.query("SELECT ' Ada ' AS name FROM (VALUES (0))")
				.fetchList(ContactProjection.class).stream().map(ContactProjection::toDomain).toList();
		assertEquals("Ada", contacts.get(0).name);
	}

	@Test
	void typedScalarSqlArraysCanBecomeListsWithoutLosingNullElements() {
		Database database = database(null);
		Long[] values = database.query("VALUES (ARRAY[1, CAST(NULL AS INTEGER), 3])")
				.fetchObject(Long[].class).orElseThrow();
		List<Long> list = Arrays.asList(values);
		assertEquals(Arrays.asList(1L, null, 3L), list);
		assertTrue(database.query("VALUES (CAST(NULL AS BIGINT ARRAY))").fetchObject(Long[].class).isEmpty());
	}

	private Database database(InstanceProvider provider) {
		JDBCDataSource source = new JDBCDataSource();
		source.setUrl("jdbc:hsqldb:mem:mapping_extensions_" + UUID.randomUUID());
		source.setUser("sa"); source.setPassword("");
		return Database.with(source, DatabaseType.GENERIC).instanceProvider(provider).build();
	}
}
