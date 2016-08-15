/*
 * Copyright 2016 by Simba Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License
 */

package org.apache.spark.examples.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by gefei on 16-8-15.
  */
object PolygonExample {

  def main(args: Array[String]): Unit = {
    val sparkConf =
      new SparkConf()
        .setAppName("PolygonExample")
        .setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)


    val df1 = sqlContext.read.format("org.apache.spark.sql.execution.datasources.spatial")
      .option("type", "geojson").load("./examples/src/main/resources/example.geo.json")

    // scalastyle:off println
    df1.collect().foreach(println)
    df1.registerTempTable("table1")

    val df2 = sqlContext.sql("SELECT * FROM table1 WHERE shape INTERSECTS " +
      "POLYGON(POINT(100.5, 0.2), POINT(106, 0.2)," +
      "POINT(106, 5), POINT(100.5, 5), POINT(100.5, 0.2))")
    println(df2.queryExecution)
    df2.foreach(println)
    // scalastyle:on println
  }
}
