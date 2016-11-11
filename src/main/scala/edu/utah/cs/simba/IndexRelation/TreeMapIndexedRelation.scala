package edu.utah.cs.simba.IndexRelation

import edu.utah.cs.simba.IndexRDD
import edu.utah.cs.simba.index.TreeMapIndex
import edu.utah.cs.simba.partitioner.RangePartition
import edu.utah.cs.simba.util.DevStub
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.expressions.{Attribute, BindReferences}
import org.apache.spark.sql.catalyst.plans.logical.Statistics
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.types.NumericType

/**
  * Created by gefei on 16-6-21.
  */
private[sql] case class TreeMapIndexedRelation(
                                                output: Seq[Attribute],
                                                child: SparkPlan,
                                                table_name: Option[String],
                                                column_keys: List[Attribute],
                                                index_name: String)(var _indexedRDD: IndexRDD = null,
                                                                    var range_bounds: Array[Double] = null)
  extends IndexedRelation with MultiInstanceRelation {
  require(column_keys.length == 1)
  require(column_keys.head.dataType.isInstanceOf[NumericType])

  if (_indexedRDD == null) {
    buildIndex()
  }

  private[sql] def buildIndex(): Unit = {
    val numShufflePartitions = DevStub.numShuffledPartitions

    val dataRDD = child.execute().map(row => {
      val eval_key = BindReferences.bindReference(column_keys.head, child.output).eval(row)
        .asInstanceOf[Double]
      (eval_key, row)
    })

    val (partitionedRDD, tmp_bounds) = RangePartition.rowPartition(dataRDD, numShufflePartitions)
    range_bounds = tmp_bounds
    val indexed = partitionedRDD.mapPartitions(iter => {
      val data = iter.toArray
      val index = TreeMapIndex(data)
      Array(IPartition(data.map(_._2), index)).iterator
    }).persist(StorageLevel.MEMORY_AND_DISK_SER)

    indexed.setName(table_name.map(n => s"$n $index_name").getOrElse(child.toString))
    _indexedRDD = indexed
  }

  override def newInstance(): IndexedRelation = {
    new TreeMapIndexedRelation(output.map(_.newInstance()), child, table_name,
      column_keys, index_name)(_indexedRDD)
      .asInstanceOf[this.type]
  }

  override def withOutput(new_output: Seq[Attribute]): IndexedRelation = {
    new TreeMapIndexedRelation(new_output, child, table_name,
      column_keys, index_name)(_indexedRDD, range_bounds)
  }

  @transient override lazy val statistics = Statistics(
    // TODO: Instead of returning a default value here, find a way to return a meaningful size
    // estimate for RDDs. See PR 1238 for more discussions.
    sizeInBytes = BigInt(DevStub.defaultSizeInBytes)
  )
}