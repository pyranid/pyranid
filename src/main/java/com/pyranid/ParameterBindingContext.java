/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import com.pyranid.DefaultPreparedStatementBinder.ParameterSqlTypeResolver;
import org.jspecify.annotations.Nullable;

import java.sql.PreparedStatement;

import static java.util.Objects.requireNonNull;

/** Shares one statement execution's metadata resolver through delegating binding callbacks. */
final class ParameterBindingContext {
	private static final ThreadLocal<Binding> CURRENT = new ThreadLocal<>();

	private ParameterBindingContext() {}

	static @Nullable ParameterSqlTypeResolver current(
			StatementContext<?> context, PreparedStatement statement) {
		Binding binding = CURRENT.get();
		return binding != null && binding.context == context && binding.statement == statement ? binding.resolver : null;
	}

	static Scope activate(StatementContext<?> context, PreparedStatement statement,
			ParameterSqlTypeResolver resolver) {
		Binding binding = new Binding(requireNonNull(context), requireNonNull(statement), requireNonNull(resolver));
		Binding previous = CURRENT.get();
		CURRENT.set(binding);
		return new Scope(binding, previous);
	}

	private record Binding(StatementContext<?> context, PreparedStatement statement,
			ParameterSqlTypeResolver resolver) {}

	static final class Scope implements AutoCloseable {
		private @Nullable Binding binding;
		private @Nullable Binding previous;

		private Scope(Binding binding, @Nullable Binding previous) {
			this.binding = binding;
			this.previous = previous;
		}

		@Override public void close() {
			if (this.binding == null) return;
			if (CURRENT.get() != this.binding)
				throw new IllegalStateException("Parameter binding scopes must close in reverse activation order");
			if (this.previous == null) CURRENT.remove();
			else CURRENT.set(this.previous);
			// A retained, completed scope must not keep its statement, context or enclosing execution alive.
			this.binding = null;
			this.previous = null;
		}
	}
}
