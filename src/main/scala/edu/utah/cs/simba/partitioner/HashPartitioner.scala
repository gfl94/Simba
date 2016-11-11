package edu.utah.cs.simba.partitioner


import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.SparkSqlSerializer
import org.apache.spark.util.MutablePair
import org.apache.spark.{Partitioner, SparkConf, SparkEnv}

/**
  * Created by dong on 1/15/16.
  * Linear Hash Partitioner with Java hashcode
  */
object HashPartition {
  def sortBasedShuffleOn: Boolean = false
  //def sortBasedShuffleOn: Boolean = SparkEnv.get.shuffleManager.isInstanceOf[SortShuffleManager]

  def apply(origin: RDD[(Any, InternalRow)], num_partitions: Int): RDD[(Any, InternalRow)] = {
    val rdd = if (sortBasedShuffleOn) {
      origin.mapPartitions {iter => iter.map(row => (row._1, row._2.copy()))}
    } else {
      origin.mapPartitions {iter =>
        val mutablePair = new MutablePair[Any, InternalRow]()
        iter.map(row => mutablePair.update(row._1, row._2.copy()))
      }
    }

    val part = new HashPartitioner(num_partitions)
    val shuffled = new ShuffledRDD[Any, InternalRow, InternalRow](rdd, part)
    //shuffled.setSerializer(new SparkSqlSerializer(new SparkConf(false)))
    shuffled
  }
}

class HashPartitioner(num_partitions: Int) extends Partitioner {
  override def numPartitions: Int = num_partitions

  override def getPartition(key: Any): Int = {
    key.hashCode() % num_partitions
  }
}