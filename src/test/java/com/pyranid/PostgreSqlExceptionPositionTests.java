/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import com.pyranid.testdriver.util.ServerErrorMessage;
import org.junit.jupiter.api.Test;
import org.postgresql.util.PSQLException;

import java.sql.BatchUpdateException;
import java.sql.SQLException;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class PostgreSqlExceptionPositionTests {
	@Test
	void omittedPgjdbcPositionFieldsAreEmptyRatherThanZero() {
		PSQLException driver = new PSQLException(new org.postgresql.util.ServerErrorMessage(
				"SERROR\0C23505\0Mduplicate key\0\0"), false);
		assertEquals(0, driver.getServerErrorMessage().getPosition());
		assertEquals(0, driver.getServerErrorMessage().getInternalPosition());
		DatabaseException failure = new DatabaseException("failed", driver, PostgresDialect.INSTANCE);
		assertTrue(failure.getPosition().isEmpty());
		assertTrue(failure.getInternalPosition().isEmpty());
		assertFalse(failure.toString().contains("position="));
		assertFalse(failure.toString().contains("internalPosition="));
		assertSame(driver, failure.getCause());
		assertEquals("23505", failure.getSqlState().orElseThrow());
		assertTrue(failure.isUniqueConstraintViolation());
	}

	@Test
	void oneBasedPositionsSurviveIndependentlyWhileNonpositiveValuesAreAbsent() {
		for (int position : new int[]{Integer.MIN_VALUE, -1, 0, 1, 37, Integer.MAX_VALUE}) {
			for (int internalPosition : new int[]{-3, 0, 1, 19}) {
				DatabaseException failure = failure(new Positions(position, internalPosition));
				assertEquals(position > 0 ? Optional.of(position) : Optional.empty(), failure.getPosition());
				assertEquals(internalPosition > 0 ? Optional.of(internalPosition) : Optional.empty(), failure.getInternalPosition());
				assertEquals(434, failure.getLine().orElseThrow());
				assertEquals("employee_email_key", failure.getConstraint().orElseThrow());
			}
		}
	}

	@Test
	void jdbcCauseAndNextWrappersRetainTheSelectedMessagesPositivePositionsAndPrimaryState() {
		for (boolean throughCause : new boolean[]{false, true}) {
			var driver = driver(new Positions(17, 9));
			BatchUpdateException wrapper = new BatchUpdateException("batch", "HY000", 37, new int[]{1});
			if (throughCause) wrapper.initCause(new IllegalStateException("intermediate", driver));
			else wrapper.setNextException(driver);
			DatabaseException failure = new DatabaseException("failed", wrapper, PostgresDialect.INSTANCE);
			assertEquals(17, failure.getPosition().orElseThrow());
			assertEquals(9, failure.getInternalPosition().orElseThrow());
			assertEquals("HY000", failure.getSqlState().orElseThrow());
			assertEquals(37, failure.getErrorCode().orElseThrow());
			assertSame(wrapper, failure.getCause());
			assertTrue(failure.isUniqueConstraintViolation());
			assertEquals(0, wrapper.getSuppressed().length);
		}
	}

	@Test
	void unreadableAccessorsDoNotHideTheOtherPositionOrRemainingServerFields() {
		for (boolean unreadablePosition : new boolean[]{false, true}) {
			ServerErrorMessage message = new Positions(17, 9) {
				@Override public int getPosition() {
					if (unreadablePosition) throw new IllegalStateException("unreadable position");
					return super.getPosition();
				}
				@Override public int getInternalPosition() {
					if (!unreadablePosition) throw new AssertionError("unreadable internal position");
					return super.getInternalPosition();
				}
			};
			DatabaseException failure = failure(message);
			assertEquals(unreadablePosition ? Optional.empty() : Optional.of(17), failure.getPosition());
			assertEquals(unreadablePosition ? Optional.of(9) : Optional.empty(), failure.getInternalPosition());
			assertEquals(434, failure.getLine().orElseThrow());
			assertEquals("employee", failure.getTable().orElseThrow());
		}
	}

	@Test
	void cyclesMissingMessagesAndSuppressedFailuresDoNotFabricateOrMixPositions() {
		var noPositions = driver(new Positions(0, 0));
		noPositions.setNextException(driver(new Positions(17, 9)));
		assertTrue(new DatabaseException("failed", noPositions, PostgresDialect.INSTANCE).getPosition().isEmpty(),
				"A later server error must not fill a position absent from the selected message");

		SQLException cycle = new SQLException("wrapper", "HY000");
		var empty = driver(null);
		cycle.setNextException(empty);
		empty.setNextException(cycle);
		cycle.addSuppressed(driver(new Positions(17, 9)));
		DatabaseException failure = new DatabaseException("failed", cycle, PostgresDialect.INSTANCE);
		assertTrue(failure.getPosition().isEmpty());
		assertTrue(failure.getInternalPosition().isEmpty());
		assertEquals("HY000", failure.getSqlState().orElseThrow());
		assertEquals(1, cycle.getSuppressed().length);
		assertTrue(new DatabaseException("failed", new IllegalArgumentException("runtime", driver(new Positions(17, 9))),
				PostgresDialect.INSTANCE).getPosition().isEmpty());
	}

	private static DatabaseException failure(ServerErrorMessage message) {
		return new DatabaseException("failed", driver(message), PostgresDialect.INSTANCE);
	}
	private static com.pyranid.testdriver.util.PSQLException driver(ServerErrorMessage message) {
		return new com.pyranid.testdriver.util.PSQLException("driver failure", "23505", message);
	}

	public static class Positions extends ServerErrorMessage {
		private final int position;
		private final int internalPosition;
		public Positions(int position, int internalPosition) {
			this.position = position;
			this.internalPosition = internalPosition;
		}
		@Override public int getPosition() { return this.position; }
		@Override public int getInternalPosition() { return this.internalPosition; }
	}
}
