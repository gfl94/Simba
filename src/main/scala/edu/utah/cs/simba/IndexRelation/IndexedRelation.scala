package edu.utah.cs.simba.IndexRelation

import edu.utah.cs.simba.IndexRDD
import edu.utah.cs.simba.index.{Index, IndexType, RTreeType, TreapType}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan

/**
  * Created by gefei on 2016/11/11.
  */

private[sql] case class IPartition(data: Array[InternalRow], index: Index)

private[sql] object IndexedRelation {
  def apply(child: SparkPlan, table_name: Option[String], index_type: IndexType,
            column_keys: List[Attribute], index_name: String): IndexedRelation = {
    index_type match {
//      case TreeMapType =>
//        new TreeMapIndexedRelation(child.output, child, table_name, column_keys, index_name)()
//      case TreapType =>
//        new TreapIndexedRelation(child.output, child, table_name, column_keys, index_name)()
      case RTreeType =>
        new RTreeIndexedRelation(child.output, child, table_name, column_keys, index_name)()
//      case HashMapType =>
//        new HashMapIndexedRelation(child.output, child, table_name, column_keys, index_name)()
//      case QuadTreeType =>
//        new QuadTreeIndexedRelation(child.output, child, table_name, column_keys, index_name)()
      case _ => null
    }
  }
}

private[sql] abstract class IndexedRelation extends LogicalPlan {
  self: Product =>
  var _indexedRDD: IndexRDD
  def indexedRDD: IndexRDD = _indexedRDD

  override def children: Seq[LogicalPlan] = Nil
  def output: Seq[Attribute]

  def withOutput(newOutput: Seq[Attribute]): IndexedRelation
}
