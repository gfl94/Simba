/*
 * Copyright 2016 by Simba Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.utah.cs.simba

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by gefei on 2016/11/26.
  */
private[simba] class TestSimbaContext(sc: SparkContext) extends SimbaContext(sc) { self =>
  def this() {
    this(new SparkContext("local[2]", "test-simba-context",
      new SparkConf().set("spark.sql.testkey", "true")))
  }

  protected[simba] override lazy val simbaConf: SimbaConf = new SimbaConf{
    override def clear(): Unit = {
      super.clear()

      TestSimbaContext.overrideConfs.map {
        case (k, v) => setConfString(k, v)
      }
    }
  }
}

private[simba] object TestSimbaContext {
  val overrideConfs: Map[String, String] =
    Map(SimbaConf.INDEX_PARTITIONS.key -> "5")
}
