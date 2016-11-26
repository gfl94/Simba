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

/**
  * Created by gefei on 2016/11/26.
  */
class SimbaConfSuite extends SimbaFunSuite with SharedSimbaContext {
  private val testKey = "simba.key.0"
  private val testVal = "simba.val.0"

  test("propagate from spark conf") {
    assert(simbaContext.getConf("spark.sql.testkey", "false") === "true")
  }

  test("programmatic ways of basic setting and getting") {
    simbaContext.setConf(testKey, testVal)
    assert(simbaContext.getConf(testKey).eq(testVal), "setting failure")

    simbaContext.simbaConf.clear()

    // only confs startWith "simba" can be cleared
    assert(simbaContext.getAllConfs.filterKeys(k => k.contains("simba")) === TestSimbaContext.overrideConfs)

    simbaContext.setConf(testKey, testVal)
    assert(simbaContext.getConf(testKey) === testVal)
    assert(simbaContext.getConf(testKey, testVal + "_") === testVal)
    assert(simbaContext.getAllConfs.contains(testKey))

    simbaContext.simbaConf.unsetConf(testKey)
    assert(!simbaContext.getAllConfs.contains(testKey))

    simbaContext.simbaConf.clear() // part of properties can not be cleared
  }
}
