/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import java.lang.reflect.Array;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** Resolves member signatures in their concrete row class without retaining a hierarchy outside its mapper metadata. */
final class GenericPropertyTypes {
	private GenericPropertyTypes() {}

	static Type resolve(Class<?> resultClass, Class<?> declaringClass, Type memberType) {
		return resolve((Type) resultClass, declaringClass, memberType);
	}

	static Type resolve(Type resultType, Class<?> declaringClass, Type memberType) {
		Map<TypeVariable<?>, Type> bindings = bindingsFor(resultType, declaringClass, Map.of(), new HashSet<>());
		return substitute(memberType, bindings == null ? Map.of() : bindings, new HashSet<>());
	}

	private static Map<TypeVariable<?>, Type> bindingsFor(Type currentType, Class<?> declaringClass,
			Map<TypeVariable<?>, Type> inheritedBindings, Set<Class<?>> path) {
		Class<?> currentClass = rawClass(currentType);
		if (currentClass == null || !path.add(currentClass)) return null;
		try {
			Map<TypeVariable<?>, Type> bindings = new HashMap<>(inheritedBindings);
			bindType(currentType, bindings);
			if (currentClass == declaringClass) return bindings;
			Type superclass = currentClass.getGenericSuperclass();
			if (superclass != null) {
				Map<TypeVariable<?>, Type> found = bindingsFor(superclass, declaringClass, bindings, path);
				if (found != null) return found;
			}
			for (Type implemented : currentClass.getGenericInterfaces()) {
				Map<TypeVariable<?>, Type> found = bindingsFor(implemented, declaringClass, bindings, path);
				if (found != null) return found;
			}
			return null;
		} finally {
			path.remove(currentClass);
		}
	}

	private static void bindType(Type type, Map<TypeVariable<?>, Type> bindings) {
		if (!(type instanceof ParameterizedType parameterized)) return;
		if (parameterized.getOwnerType() != null) bindType(parameterized.getOwnerType(), bindings);
		Class<?> raw = rawClass(parameterized);
		if (raw == null) return;
		TypeVariable<?>[] variables = raw.getTypeParameters();
		Type[] arguments = parameterized.getActualTypeArguments();
		// Resolve against the inherited environment before replacing this declaration's variables.
		Type[] resolved = new Type[arguments.length];
		for (int index = 0; index < arguments.length; index++)
			resolved[index] = substitute(arguments[index], bindings, new HashSet<>());
		for (int index = 0; index < variables.length; index++) bindings.put(variables[index], resolved[index]);
	}

	private static Class<?> rawClass(Type type) {
		if (type instanceof Class<?> raw) return raw;
		if (type instanceof ParameterizedType parameterized && parameterized.getRawType() instanceof Class<?> raw) return raw;
		return null;
	}

	private static Type substitute(Type type, Map<TypeVariable<?>, Type> bindings, Set<TypeVariable<?>> resolving) {
		if (type instanceof TypeVariable<?> variable) {
			Type value = bindings.get(variable);
			if (value == null || value.equals(variable) || !resolving.add(variable)) return variable;
			try { return substitute(value, bindings, resolving); }
			finally { resolving.remove(variable); }
		}
		if (type instanceof ParameterizedType parameterized) {
			Type owner = parameterized.getOwnerType();
			Type resolvedOwner = owner == null ? null : substitute(owner, bindings, resolving);
			Type[] arguments = parameterized.getActualTypeArguments();
			Type[] resolved = substitute(arguments, bindings, resolving);
			if (Arrays.equals(arguments, resolved) && java.util.Objects.equals(owner, resolvedOwner)) return type;
			return new DefaultParameterizedType(parameterized.getRawType(), resolved, resolvedOwner);
		}
		if (type instanceof GenericArrayType array) {
			Type component = substitute(array.getGenericComponentType(), bindings, resolving);
			if (component instanceof Class<?> raw) return Array.newInstance(raw, 0).getClass();
			return component.equals(array.getGenericComponentType()) ? type : new ResolvedArrayType(component);
		}
		if (type instanceof WildcardType wildcard) {
			Type[] upper = wildcard.getUpperBounds(), lower = wildcard.getLowerBounds();
			Type[] resolvedUpper = substitute(upper, bindings, resolving), resolvedLower = substitute(lower, bindings, resolving);
			return Arrays.equals(upper, resolvedUpper) && Arrays.equals(lower, resolvedLower)
					? type : new ResolvedWildcardType(resolvedUpper, resolvedLower);
		}
		return type;
	}

	private static Type[] substitute(Type[] types, Map<TypeVariable<?>, Type> bindings, Set<TypeVariable<?>> resolving) {
		Type[] resolved = new Type[types.length];
		for (int index = 0; index < types.length; index++) resolved[index] = substitute(types[index], bindings, resolving);
		return resolved;
	}

	static boolean hasUnresolvedVariable(Type type) {
		if (type instanceof TypeVariable<?>) return true;
		if (type instanceof ParameterizedType parameterized) {
			if (parameterized.getOwnerType() != null && hasUnresolvedVariable(parameterized.getOwnerType())) return true;
			for (Type argument : parameterized.getActualTypeArguments()) if (hasUnresolvedVariable(argument)) return true;
		} else if (type instanceof GenericArrayType array) {
			return hasUnresolvedVariable(array.getGenericComponentType());
		} else if (type instanceof WildcardType wildcard) {
			for (Type bound : wildcard.getUpperBounds()) if (hasUnresolvedVariable(bound)) return true;
			for (Type bound : wildcard.getLowerBounds()) if (hasUnresolvedVariable(bound)) return true;
		}
		return false;
	}

	private record ResolvedArrayType(Type component) implements GenericArrayType {
		@Override public Type getGenericComponentType() { return this.component; }
		@Override public boolean equals(Object other) {
			return other instanceof GenericArrayType array && this.component.equals(array.getGenericComponentType());
		}
		@Override public int hashCode() { return this.component.hashCode(); }
		@Override public String getTypeName() { return this.component.getTypeName() + "[]"; }
		@Override public String toString() { return getTypeName(); }
	}

	private static final class ResolvedWildcardType implements WildcardType {
		private final Type[] upper;
		private final Type[] lower;
		private ResolvedWildcardType(Type[] upper, Type[] lower) {
			this.upper = upper.clone();
			this.lower = lower.clone();
		}
		@Override public Type[] getUpperBounds() { return this.upper.clone(); }
		@Override public Type[] getLowerBounds() { return this.lower.clone(); }
		@Override public boolean equals(Object other) {
			return other instanceof WildcardType wildcard && Arrays.equals(this.upper, wildcard.getUpperBounds())
					&& Arrays.equals(this.lower, wildcard.getLowerBounds());
		}
		@Override public int hashCode() { return Arrays.hashCode(this.upper) ^ Arrays.hashCode(this.lower); }
		@Override public String getTypeName() {
			if (this.lower.length != 0) return "? super " + names(this.lower);
			return this.upper.length == 0 || Arrays.equals(this.upper, new Type[]{Object.class}) ? "?" : "? extends " + names(this.upper);
		}
		@Override public String toString() { return getTypeName(); }
		private static String names(Type[] types) {
			return Arrays.stream(types).map(Type::getTypeName).collect(Collectors.joining(" & "));
		}
	}
}
