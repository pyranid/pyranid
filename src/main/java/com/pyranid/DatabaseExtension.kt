package com.pyranid

import kotlin.reflect.KClass


fun <T : Any> Query.fetchObject(
    klass: KClass<T>
): T? = fetchObject(klass.java).orElse(null)

fun <T : Any> Query.fetchList(
    klass: KClass<T>
): List<T> = fetchList(klass.java)

fun <T : Any> Query.executeForObject(
    klass: KClass<T>
): T? = executeForObject(klass.java).orElse(null)

fun <T : Any> Query.executeForList(
    klass: KClass<T>
): List<T> = executeForList(klass.java)
