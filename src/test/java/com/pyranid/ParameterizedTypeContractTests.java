/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2026 Revetware LLC.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may obtain a copy at https://www.apache.org/licenses/LICENSE-2.0
 */
package com.pyranid;

import org.junit.jupiter.api.Test;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class ParameterizedTypeContractTests {
	private static class Owner<T> { class Member<U> {} }
	private static class Types {
		List<String> plain;
		Map<String, List<Long>> nested;
		Owner<String>.Member<Integer> owned;
		Map.Entry<String, Long> staticMember;
		List<? extends Number> upperWildcard;
		List<? super Integer> lowerWildcard;
		Map<String, List<String>[]> genericArray;
		List<UUID> uuidList;
		Set<UUID> uuidSet;
		Map<String, Integer> integerMap;
	}

	@Test
	void substitutedTypesCompareAndHashLikeReflectionIncludingNestedOwnersAndArguments() throws Exception {
		for (String field : List.of("plain", "nested", "owned", "staticMember", "upperWildcard", "lowerWildcard", "genericArray")) {
			ParameterizedType reflected = reflected(field);
			ParameterizedType generated = copy(reflected);
			assertEquals(reflected, generated, field);
			assertEquals(generated, reflected, field);
			assertEquals(reflected.hashCode(), generated.hashCode(), field);
			assertEquals("reflected", keyed(reflected, "reflected").get(generated), field);
			assertEquals("generated", keyed(generated, "generated").get(reflected), field);
			assertEquals("target", keyed(TargetType.of(reflected), "target").get(TargetType.of(generated)), field);
		}
	}

	@Test
	void constructorAndAccessorArraysCannotMutateTypeEqualityOrHashKeys() throws Exception {
		ParameterizedType reflected = reflected("plain");
		Type[] supplied = {String.class};
		ParameterizedType generated = new DefaultParameterizedType(List.class, supplied, null);
		Map<Type, String> cached = keyed(generated, "stable");
		int hash = generated.hashCode();
		supplied[0] = UUID.class;
		Type[] exposed = generated.getActualTypeArguments();
		exposed[0] = Long.class;
		assertEquals(String.class, generated.getActualTypeArguments()[0]);
		assertEquals(reflected, generated);
		assertEquals(hash, generated.hashCode());
		assertEquals("stable", cached.get(reflected));
	}

	@Test
	void typedParameterTokensAreInterchangeableWithReflectedCustomBinderKeys() throws Exception {
		assertTypeKey(reflected("uuidList"), Parameters.listOf(UUID.class, List.of(UUID.randomUUID())));
		assertTypeKey(reflected("uuidSet"), Parameters.setOf(UUID.class, Set.of(UUID.randomUUID())));
		assertTypeKey(reflected("integerMap"), Parameters.mapOf(String.class, Integer.class, Map.of("one", 1)));
	}

	private static void assertTypeKey(Type reflected, TypedParameter parameter) {
		Type explicit = parameter.getExplicitType();
		assertEquals(reflected, explicit);
		assertEquals(explicit, reflected);
		assertEquals(reflected.hashCode(), explicit.hashCode());
		assertEquals("binder", keyed(reflected, "binder").get(explicit));
		assertEquals("binder", keyed(explicit, "binder").get(reflected));
	}

	private static <T> Map<T, String> keyed(T key, String value) {
		Map<T, String> result = new HashMap<>();
		result.put(key, value);
		return result;
	}

	private static ParameterizedType reflected(String field) throws Exception {
		return (ParameterizedType) Types.class.getDeclaredField(field).getGenericType();
	}

	private static ParameterizedType copy(ParameterizedType source) {
		Type[] arguments = source.getActualTypeArguments();
		for (int index = 0; index < arguments.length; index++)
			if (arguments[index] instanceof ParameterizedType parameterized) arguments[index] = copy(parameterized);
		Type owner = source.getOwnerType();
		if (owner instanceof ParameterizedType parameterized) owner = copy(parameterized);
		return new DefaultParameterizedType(source.getRawType(), arguments, owner);
	}
}
