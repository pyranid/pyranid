package com.pyranid

import java.sql.ResultSet
import java.time.ZoneId
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import kotlin.reflect.KClass
import kotlin.reflect.KFunction
import kotlin.reflect.KParameter
import kotlin.reflect.full.primaryConstructor

/**
 * @author Casey Watson
 * @since 1.0.16
 */

/**
 * Creates a {@code ResultSetMapper} for the given {@code databaseType} and {@code instanceProvider}.
 *@param javaDefaultResultSetMapper
 *          an instance of a result set mapper for standard java classes
 * @param databaseType
 *          the type of database we're working with
 * @param instanceProvider
 *          instance-creation factory, used to instantiate resultset row objects as needed
 * @param timeZone
 *          the timezone to use when working with {@link java.sql.Timestamp} and similar values
 * @since 1.0.16
 */
open class KotlinDefaultResultSetMapper(private val javaDefaultResultSetMapper: ResultSetMapper,
                                        private val databaseType: DatabaseType,
                                        private val instanceProvider: InstanceProvider,
                                        private val timeZone: ZoneId) : ResultSetMapper {

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
    constructor(javaDefaultResultSetMapper: ResultSetMapper, databaseType: DatabaseType, instanceProvider: InstanceProvider)
            : this(javaDefaultResultSetMapper, databaseType, instanceProvider, ZoneId.systemDefault())

    private val timeZoneCalendar = Calendar.getInstance(TimeZone.getTimeZone(timeZone))
    private val columnNamesForParameterCache = ConcurrentHashMap<KClass<out Any>, Map<String, Set<String>>>()
    private val parameterNamesForDataClassCache = ConcurrentHashMap<KClass<out Any>, CtorParameters>()
    private val kotlinClassForJavaClass = ConcurrentHashMap<Class<out Any>, KClass<out Any>>()

    data class CtorParameters(val ctor: KFunction<Any>, val ctorParameters: List<ParameterName>)
    data class ParameterName(val name: String, val parameter: KParameter)

    override fun <T : Any> map(resultSet: ResultSet, resultClass: Class<T>): T {

        val klass = kotlinClassForJavaClass.computeIfAbsent(resultClass) {
            Class.forName(resultClass.name).kotlin
        }

        return if (klass.isData) {
            mapKotlinDataClass(resultSet, klass)
        } else {
            javaDefaultResultSetMapper.map(resultSet, resultClass)
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
                    val parameterNames: List<ParameterName> = ctor.parameters.map { parameter ->
                        if (parameter.name == null) {
                            throw DatabaseException("Parameter was not readable for ${dataClass.simpleName}. Examples of nameless parameters include this instance for member functions, extension receiver for extension functions or properties, parameters of Java methods compiled without the debug information, and others.")
                        }
                        return@map ParameterName(parameter.name!!, parameter)
                    }.toList()
                    return@computeIfAbsent CtorParameters(ctor, parameterNames)
                }

        val columnNamesForParameters: Map<String, Set<String>> = columnNamesForParameterCache.computeIfAbsent(resultClass) {
            ctorParameters.map { parameter -> parameter.name to databaseColumnNamesForParameterName(parameter.name) }.toMap()
        }

        val callByArgs = ctorParameters
                .map { ctorParameter ->
                    val possibleColumnNamesForParameter: Set<String> = columnNamesForParameters[ctorParameter.name]
                            ?: throw DatabaseException("Unable to find columns for parameter name ${ctorParameter.name}")

                    return@map ctorParameter.parameter to columnLabelsToValues.filter { columnLabelValues ->
                        possibleColumnNamesForParameter.contains(columnLabelValues.key)
                    }.map { it.value }.firstOrNull()

                }.filter { it.second != null }
                .toMap()
        return ctor.callBy(callByArgs) as T
    }


    /**
     * Massages a Data Class property name to match standard database column name (camelCase -> camel_case).
     * <p>
     * Uses {@link #normalizationLocale()} to perform case-changing.
     * <p>
     * There may be multiple database column name mappings, for example property {@code address1} might map to both
     * {@code address1} and {@code address_1} column names.
     *
     * @param parameterName
     *          the Data Class property name to massage
     * @return the column names that match the Data Class property name
     */

    protected fun databaseColumnNamesForParameterName(parameterName: String): Set<String> {
        val normalizedPropertyNames: MutableSet<String> = HashSet(2)
        // Converts camelCase to camel_case
        val camelCaseRegex = "([a-z])([A-Z]+)"
        val replacement = "$1_$2"
        val normalizedPropertyName = parameterName.replace(camelCaseRegex.toRegex(), replacement).toLowerCase(normalizationLocale())
        normalizedPropertyNames.add(normalizedPropertyName)
        // Converts address1 to address_1
        val letterFollowedByNumberRegex = "(\\D)(\\d)"
        val normalizedNumberPropertyName = normalizedPropertyName.replace(letterFollowedByNumberRegex.toRegex(), replacement)
        normalizedPropertyNames.add(normalizedNumberPropertyName)
        return normalizedPropertyNames.toSet()

    }

    protected fun normalizeColumnLabel(columnLabel: String): String {
        return columnLabel.toLowerCase(normalizationLocale())
    }

    protected fun normalizationLocale(): Locale {
        return Locale.ENGLISH
    }

    protected fun mapResultSetValueToObject(resultSet: ResultSet, columnLabel: String): Any {
        val obj = resultSet.getObject(columnLabel)
        return when (obj) {
            is java.sql.Timestamp -> resultSet.getTimestamp(columnLabel, timeZoneCalendar)
            is java.sql.Date -> resultSet.getDate(columnLabel, timeZoneCalendar)
            is java.sql.Time -> resultSet.getTime(columnLabel, timeZoneCalendar)
            else -> obj
        }
    }
}
