package database.layer_6_query

import database.layer_6_query.Expression.Column
import database.layer_6_query.Expression.Const
import database.layer_4_record.Schema
import database.types.ColumnName

/**
 * A single term of a [Predicate].
 *
 * Currently only equality of expressions is supported.
 */
class Term(private val lhs: Expression, private val rhs: Expression) {

    fun reductionFactor(plan: Plan): Int = when {
        lhs is Column && rhs is Column ->
            maxOf(plan.distinctValues(lhs.fieldName), plan.distinctValues(rhs.fieldName))
        lhs is Column ->
            plan.distinctValues(lhs.fieldName)
        rhs is Column ->
            plan.distinctValues(rhs.fieldName)
        lhs is Const && rhs is Const && lhs.value == rhs.value ->
            1
        else ->
            Integer.MAX_VALUE
    }

    fun equatesWithConstant(fieldName: ColumnName): SqlValue? = when {
        lhs is Column && rhs is Const && lhs.fieldName == fieldName ->
            rhs.value
        rhs is Column && lhs is Const && rhs.fieldName == fieldName ->
            lhs.value
        else ->
            null
    }

    fun equatesWithField(fieldName: ColumnName): ColumnName? = when {
        lhs is Column && rhs is Column && lhs.fieldName == fieldName ->
            rhs.fieldName
        lhs is Column && rhs is Column && rhs.fieldName == fieldName ->
            lhs.fieldName
        else ->
            null
    }

    fun appliesTo(schema: Schema): Boolean =
        lhs.appliesTo(schema) && rhs.appliesTo(schema)

    fun isSatisfied(scan: Scan): Boolean =
        lhs.evaluate(scan) == rhs.evaluate(scan)

    override fun toString(): String =
        "$lhs=$rhs"
}
