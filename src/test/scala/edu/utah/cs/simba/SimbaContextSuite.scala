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

import org.apache.spark.sql.AnalysisException

class SimbaContextSuite extends SimbaFunSuite with SharedSparkContext {
  test("getOrCreate instantiates SimbaContext") {
    val simbaContext = SimbaContext.getOrCreate(sc)
    assert(simbaContext != null, "SimbaContext.getOrCreate returned null")
    val temp = SimbaContext.getOrCreate(sc)
    println(temp)
    println(simbaContext)
    assert(SimbaContext.getOrCreate(sc).eq(simbaContext),
      "SimbaContext created by SimbaContext.getOrCreate not returned by SimbaContext.getOrCreate")
  }

  test("getOrCreate return original SQLContext") {
    val simbaContext = SimbaContext.getOrCreate(sc)
    val newSession  = simbaContext.newSession()
    assert(SimbaContext.getOrCreate(sc).eq(simbaContext),
      "SimbaContext.getOrCreate after explicitly created SimbaContext did not return the context")
    SimbaContext.setActive(newSession)
    assert(SimbaContext.getOrCreate(sc).eq(newSession),
      "SimbaContext.getOrCreate after explicitly setActive() did not return the active context")
  }

  test("Sessions of SimbaContext") {
    val simbaContext = SimbaContext.getOrCreate(sc)
    val session1 = simbaContext.newSession()
    val session2 = simbaContext.newSession()

    val key = SimbaConf.INDEX_PARTITIONS.key
    assert(session1.getConf(key) === session2.getConf(key))
    session1.setConf(key, "1")
    session2.setConf(key, "2")
    assert(session1.getConf(key) === "1")
    assert(session2.getConf(key) === "2")

    val df = session1.range(10)
    df.registerTempTable("test1")
    assert(session1.tableNames().contains("test1"))
    assert(!session2.tableNames().contains("test1"))

    def myadd(a: Int, b: Int): Int = a + b + 1
    session1.udf.register[Int, Int, Int]("myadd", myadd)
    session1.sql("select myadd(1, 2)").explain()
    intercept[AnalysisException] {
      session2.sql("select myadd(1, 2)").explain()
    }
  }
}
