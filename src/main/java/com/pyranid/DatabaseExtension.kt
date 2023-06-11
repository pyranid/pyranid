package com.pyranid

import kotlin.reflect.KClass


fun <T : Any> Database.queryForObject(
    sql: String,
    klass: KClass<T>,
    vararg parameters: Any
): T? {
    return queryForObject(sql, klass.java, *parameters).orElse(null)
}


fun <T : Any> Database.queryForList(
    sql: String,
    klass: KClass<T>,
    vararg parameters: Any
): List<T> {
    val dbResults = queryForList(sql, klass.java, *parameters)
    return dbResults
}

fun <T : Any> Database.queryForList(
    sql: String,
    klass: KClass<T>
): List<T> {
    val dbResults = queryForList(sql, klass.java)
    return dbResults
}

