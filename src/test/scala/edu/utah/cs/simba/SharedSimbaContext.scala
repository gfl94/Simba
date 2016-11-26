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

import org.apache.spark.SparkContext
import org.scalatest.{BeforeAndAfterAll, Suite}

/**
  * Created by gefei on 2016/11/26.
  */
trait SharedSimbaContext extends BeforeAndAfterAll { self: Suite =>
  private var _ctx: TestSimbaContext = null

  protected def simbaContext: SimbaContext = _ctx

  protected override def beforeAll(): Unit = {
    if (_ctx == null) {
      _ctx = new TestSimbaContext()
    }
    super.beforeAll()
  }

  protected override def afterAll(): Unit = {
    try {
      if (_ctx != null) {
        _ctx.sparkContext.stop()
        _ctx = null
      }
    } finally {
      super.afterAll()
    }
  }
}