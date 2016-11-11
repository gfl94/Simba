package edu.utah.cs.simba.util

import edu.utah.cs.simba.spatial.Point
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, BindReferences}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan

/**
  * Created by zhongpu on 16-7-20.
  */
object FetchPointUtils {

  def getFromRow(row: InternalRow, columns: List[Attribute], plan: LogicalPlan,
                 isPoint: Boolean): Point = {
    if (isPoint) {
      BindReferences.bindReference(columns.head, plan.output).eval(row)
        .asInstanceOf[Point]
    } else {
      Point(columns.toArray.map(BindReferences.bindReference(_, plan.output).eval(row)
        .asInstanceOf[Number].doubleValue()))
    }
  }

  def getFromRow(row: InternalRow, columns: List[Attribute], plan: SparkPlan,
                 isPoint: Boolean): Point = {
    if (isPoint) {
      BindReferences.bindReference(columns.head, plan.output).eval(row)
        .asInstanceOf[Point]
    } else {
      Point(columns.toArray.map(BindReferences.bindReference(_, plan.output).eval(row)
        .asInstanceOf[Number].doubleValue()))
    }
  }
}
