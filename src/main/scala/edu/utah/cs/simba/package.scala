package edu.utah.cs

import edu.utah.cs.simba.IndexRelation.IPartition

/**
  * Created by gefei on 2016/11/11.
  */
package object simba {
  type IndexRDD = org.apache.spark.rdd.RDD[IPartition]
}
