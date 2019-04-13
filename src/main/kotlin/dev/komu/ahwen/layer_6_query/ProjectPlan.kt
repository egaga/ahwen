package dev.komu.ahwen.layer_6_query

import dev.komu.ahwen.types.ColumnName

/**
 * Performs projection: takes [columns] from input and discards the rest
 */
class ProjectPlan(private val plan: Plan, private val columns: Collection<ColumnName>) : Plan by plan {

    override val schema = plan.schema.project(columns)

    override fun open(): Scan =
        ProjectScan(plan.open(), columns)
}
