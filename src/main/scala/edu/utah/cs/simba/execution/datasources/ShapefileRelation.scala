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

package edu.utah.cs.simba.execution.datasources

import com.vividsolutions.jts.geom.{GeometryFactory, Point => JTSPoint, Polygon => JTSPolygon}
import edu.utah.cs.simba.ShapeType
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedFilteredScan}
import org.apache.spark.sql.types.{StructField, StructType}

class ShapefileRelation(path: String)(@transient val sqlContext: SQLContext)
  extends BaseRelation with PrunedFilteredScan {

  override val schema = {
    StructType(StructField("shape", ShapeType, true) :: Nil)
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val gf = new GeometryFactory()
    val shapes = ShapeFile.Parser(path)(gf).map(_.g match {
      case p: JTSPolygon => new edu.utah.cs.simba.spatial.Polygon(p)
      case p: JTSPoint => edu.utah.cs.simba.spatial.Point(p)
    })

    sqlContext.sparkContext.parallelize(shapes).mapPartitions(part => part.map(Row(_)))
  }
}
