/*
 * Copyright 2015-2022 Transmogrify LLC, 2022-2023 Revetware LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pyranid

import java.math.BigDecimal
import java.math.BigInteger
import java.sql.ResultSet
import java.sql.Timestamp
import java.time.*
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import kotlin.reflect.KClass
import kotlin.reflect.KFunction
import kotlin.reflect.KParameter
import kotlin.reflect.full.functions
import kotlin.reflect.full.isSuperclassOf
import kotlin.reflect.full.primaryConstructor

/**
 * Creates a {@code ResultSetMapper} for the given {@code databaseType} and {@code instanceProvider}.
 *
 * @param databaseType
 *          the type of database we're working with
 * @param instanceProvider
 *          instance-creation factory, used to instantiate resultset row objects as needed
 * @param timeZone
 *          the timezone to use when working with {@link java.sql.Timestamp} and similar values
 * @author Casey Watson
 * @since 1.0.16
 */
open class KotlinDefaultResultSetMapper(
    private val javaDefaultResultSetMapper: ResultSetMapper,
    private val databaseType: DatabaseType,
    private val instanceProvider: InstanceProvider,
    private val timeZone: ZoneId
) : ResultSetMapper {

    /**
     * Creates a {@code ResultSetMapper} for the given {@code databaseType} and {@code instanceProvider}.
     *@param javaDefaultResultSetMapper
     *          ResultSetMapper to use in the case the requested class is a non-data class, such as a JavaBean
     * @param databaseType
     *          the type of database we're working with
     * @param instanceProvider
     *          instance-creation factory, used to instantiate resultset row objects as needed
     * @since 1.0.16
     */
    constructor(
        javaDefaultResultSetMapper: ResultSetMapper,
        databaseType: DatabaseType,
        instanceProvider: InstanceProvider
    )
            : this(javaDefaultResultSetMapper, databaseType, instanceProvider, ZoneId.systemDefault())

    private val timeZoneCalendar = Calendar.getInstance(TimeZone.getTimeZone(timeZone))
    private val columnNamesForParameterCache = ConcurrentHashMap<KClass<out Any>, Map<String, Set<String>>>()
    private val parameterNamesForDataClassCache = ConcurrentHashMap<KClass<out Any>, CtorParameters>()
    private val kotlinClassForJavaClass = ConcurrentHashMap<Class<out Any>, KClass<out Any>>()

    data class CtorParameters(val ctor: KFunction<Any>, val ctorParameters: List<ParameterMetadata>)
    data class ParameterMetadata(val name: String, val parameter: KParameter, val parameterType: KClass<*>)


    override fun <T : Any> map(resultSet: ResultSet, statementContext: StatementContext<T>): T {
        val resultClass = statementContext.resultType.get();

        val klass = kotlinClassForJavaClass.computeIfAbsent(resultClass) {
            Class.forName(resultClass.name).kotlin
        }

        return if (klass.isData) {
            mapKotlinDataClass(resultSet, klass)
        } else {
            javaDefaultResultSetMapper.map(resultSet, statementContext)
        }
    }

    /**
     * Attempts to map the current {@code resultSet} row to an instance of {@code resultClass}, which should be a
     * Data class.
     * <p>
     * The {@code resultClass} instance will be created via {@link #callByArgs()}.
     *
     * @param <T>
     *          result instance type token
     * @param resultSet
     *          provides raw row data to pull from
     * @param resultClass
     *          the type of instance to map to
     * @return the result of the mapping
     * @throws Exception
     *           if an error occurs during mapping
     */
    protected fun <T : Any> mapKotlinDataClass(resultSet: ResultSet, resultClass: KClass<out Any>): T {
        val metadata = resultSet.metaData
        val columnLabels = (1..metadata.columnCount).map { metadata.getColumnLabel(it) }

        val columnLabelsToValues = columnLabels
            .map { normalizeColumnLabel(it) to mapResultSetValueToObject(resultSet, it) }
            .toMap()

        val (ctor, ctorParameters) =
            parameterNamesForDataClassCache.computeIfAbsent(resultClass) { dataClass ->
                val ctor = dataClass.primaryConstructor
                    ?: throw DatabaseException("Missing primary constructor for class ${resultClass.simpleName}")
                val parameterNames: List<ParameterMetadata> = ctor.parameters.map { parameter ->
                    if (parameter.name == null) {
                        throw DatabaseException("Parameter was not readable for ${dataClass.simpleName}. Examples of nameless parameters include this instance for member functions, extension receiver for extension functions or properties, parameters of Java methods compiled without the debug information, and others.")
                    }
                    return@map ParameterMetadata(parameter.name!!, parameter, parameter.type.classifier as KClass<*>)
                }.toList()
                return@computeIfAbsent CtorParameters(ctor, parameterNames)
            }

        val columnNamesForParameters: Map<String, Set<String>> =
            columnNamesForParameterCache.computeIfAbsent(resultClass) {
                ctorParameters.map { parameter -> parameter.name to databaseColumnNamesForParameterName(parameter.name) }
                    .toMap()
            }

        val callByArgs = ctorParameters
            .map { ctorParameter ->
                val possibleColumnNamesForParameter: Set<String> = columnNamesForParameters[ctorParameter.name]
                    ?: throw DatabaseException("Unable to find columns for parameter name ${ctorParameter.name}")
                return@map ctorParameter.parameter to columnLabelsToValues
                    .filter { columnLabelValues ->
                        possibleColumnNamesForParameter.contains(columnLabelValues.key) && columnLabelValues.value != null
                    }.map {
                        convertResultSetValueToPropertyType(it.value!!, ctorParameter.parameterType)
                            ?: throw DatabaseException(
                                "Property ${it.key} of ${resultClass} has a write " +
                                        "method of type ${ctorParameter.parameterType.simpleName}, " +
                                        "but the ResultSet type ${it.value!!::class.simpleName} does not match. " +
                                        "Consider creating your own ${KotlinDefaultResultSetMapper::class.simpleName} and " +
                                        "overriding convertResultSetValueToPropertyType() to detect instances of " +
                                        "${it.value!!::class.simpleName} and convert them to ${ctorParameter.parameterType.simpleName}"
                            )
                    }.firstOrNull()

            }.filter {
                it.second != null
            }
            .toMap()

        try {
            return ctor.callBy(callByArgs) as T
        } catch (e: Exception) {
            throw DatabaseException(
                "Unable to instantiate class ${resultClass.simpleName} with parameters and arguments ${
                    callByArgs.map { it.key.name to it.value }
                        .joinToString(separator = ", ") { "${it.first}: ${it.second}" }
                }", e
            )
        }
    }


    /**
     * Massages a {@link ResultSet#getObject(String)} value to match the given {@code propertyType}.
     * <p>
     * For example, the JDBC driver might give us {@link java.sql.Timestamp} but our corresponding JavaBean field is of
     * type {@link java.util.Date}, so we need to manually convert that ourselves.
     *
     * @param resultSetValue
     *          the value returned by {@link ResultSet#getObject(String)}
     * @param propertyType
     *          the JavaBean property type we'd like to map {@code resultSetValue} to
     * @return a representation of {@code resultSetValue} that is of type {@code propertyType}
     */
    protected fun convertResultSetValueToPropertyType(resultSetValue: Any, propertyType: KClass<*>): Any? {

        if (resultSetValue is BigDecimal) {
            val bigDecimal = resultSetValue
            if (BigDecimal::class.isSuperclassOf(propertyType)) return bigDecimal
            if (BigInteger::class.isSuperclassOf(propertyType)) return bigDecimal.toBigInteger()
        }

        if (resultSetValue is BigInteger) {
            val bigInteger = resultSetValue
            if (BigDecimal::class.isSuperclassOf(propertyType)) return BigDecimal(bigInteger)
            if (BigInteger::class.isSuperclassOf(propertyType)) return bigInteger
        }

        if (resultSetValue is Number) {
            val number = resultSetValue
            if (Byte::class.isSuperclassOf(propertyType)) return number.toByte()
            if (Short::class.isSuperclassOf(propertyType)) return number.toShort()
            if (Int::class.isSuperclassOf(propertyType)) return number.toInt()
            if (Long::class.isSuperclassOf(propertyType)) return number.toLong()
            if (Float::class.isSuperclassOf(propertyType)) return number.toFloat()
            if (Double::class.isSuperclassOf(propertyType)) return number.toDouble()
            if (BigDecimal::class.isSuperclassOf(propertyType)) return BigDecimal(number.toDouble())
            if (BigInteger::class.isSuperclassOf(propertyType)) return BigDecimal(number.toDouble()).toBigInteger()
        } else if (resultSetValue is Timestamp) {
            val date = resultSetValue
            if (Date::class.isSuperclassOf(propertyType)) return date
            if (Instant::class.isSuperclassOf(propertyType)) return date.toInstant()
            if (LocalDate::class.isSuperclassOf(propertyType)) return date.toInstant().atZone(timeZone).toLocalDate()
            if (LocalDateTime::class.isSuperclassOf(propertyType)) return date.toLocalDateTime()
        } else if (resultSetValue is java.sql.Date) {
            val date = resultSetValue
            if (Date::class.isSuperclassOf(propertyType)) return date
            if (Instant::class.isSuperclassOf(propertyType)) return date.toInstant()
            if (LocalDate::class.isSuperclassOf(propertyType)) return date.toLocalDate()
            if (LocalDateTime::class.isSuperclassOf(propertyType)) return LocalDateTime.ofInstant(
                date.toInstant(),
                timeZone
            )
        } else if (resultSetValue is java.sql.Time) {
            if (LocalTime::class.isSuperclassOf(propertyType)) return resultSetValue.toLocalTime()
        } else if (ZoneId::class.isSuperclassOf(propertyType)) {
            return ZoneId.of(resultSetValue.toString())
        } else if (TimeZone::class.isSuperclassOf(propertyType)) {
            return TimeZone.getTimeZone(resultSetValue.toString())
        } else if (Locale::class.isSuperclassOf(propertyType)) {
            return Locale.forLanguageTag(resultSetValue.toString())
        } else if (Enum::class.isSuperclassOf(propertyType)) {
            return propertyType.functions.first { func -> func.name == "valueOf" }.call(resultSetValue)
        } else if ("org.postgresql.util.PGobject" == resultSetValue.javaClass.name) {
            val pgObject = resultSetValue as org.postgresql.util.PGobject
            return pgObject.value
        }
        return resultSetValue
    }


    /**
     * Massages a Data Class property name to match standard database column name (camelCase -> camel_case).
     * <p>
     * Uses {@link #normalizationLocale()} to perform case-changing.
     * <p>
     * There may be multiple database column name mappings, for example property {@code address1} might map to both
     * {@code address1} and {@code address_1} column names.
     *
     * @param propertyName
     *          the Data Class property name to massage
     * @return the column names that match the Data Class property name
     */

    protected fun databaseColumnNamesForParameterName(propertyName: String): Set<String> {
        val normalizedPropertyNames: MutableSet<String> = HashSet(2)
        // Converts camelCase to camel_case
        val camelCaseRegex = "([a-z])([A-Z]+)"
        val replacement = "$1_$2"
        val normalizedPropertyName =
            propertyName.replace(camelCaseRegex.toRegex(), replacement).lowercase(normalizationLocale())
        normalizedPropertyNames.add(normalizedPropertyName)
        // Converts address1 to address_1
        val letterFollowedByNumberRegex = "(\\D)(\\d)"
        val normalizedNumberPropertyName =
            normalizedPropertyName.replace(letterFollowedByNumberRegex.toRegex(), replacement)
        normalizedPropertyNames.add(normalizedNumberPropertyName)
        return normalizedPropertyNames.toSet()
    }

    protected fun normalizeColumnLabel(columnLabel: String): String {
        return columnLabel.lowercase(normalizationLocale())
    }

    protected fun normalizationLocale(): Locale {
        return Locale.ENGLISH
    }

    protected fun mapResultSetValueToObject(resultSet: ResultSet, columnLabel: String): Any? {
        val obj = resultSet.getObject(columnLabel) ?: return null
        return when (obj) {
            is java.sql.Timestamp -> resultSet.getTimestamp(columnLabel, timeZoneCalendar)
            is java.sql.Date -> resultSet.getDate(columnLabel, timeZoneCalendar)
            is java.sql.Time -> resultSet.getTime(columnLabel, timeZoneCalendar)
            else -> obj
        }
    }
}
