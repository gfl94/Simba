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
 *
 */

package edu.utah.cs.simba

import java.util.Properties
import java.util.concurrent.atomic.AtomicReference

import org.apache.spark.SparkContext
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.SQLContext

import scala.collection.JavaConverters._
import scala.collection.immutable

/**
  * Created by dongx on 11/11/16.
  */
class SimbaContext(sc: SparkContext) extends SQLContext(sc) {
  self =>
  def this(sparkContext: JavaSparkContext) = this(sparkContext.sc)

  protected[simba] lazy val simbaConf = new SimbaConf

  override def setConf(props: Properties): Unit = {
    props.asScala.foreach { case (k, v) => setConf(k, v) }
  }

  override def setConf(key: String, value: String): Unit = {
    if (key.startsWith("simba.")) simbaConf.setConfString(key, value)
    else conf.setConfString(key, value)
  }

  override def getConf(key: String): String = {
    if (key.startsWith("simba.")) simbaConf.getConfString(key)
    else conf.getConfString(key)
  }

  override def getConf(key: String, defaultValue: String): String = {
    if (key.startsWith("simba.")) conf.getConfString(key, defaultValue)
    else conf.getConfString(key, defaultValue)
  }

  override def getAllConfs: immutable.Map[String, String] = {
    conf.getAllConfs ++ simbaConf.getAllConfs
  }
}

object SimbaContext {
  private val activeContext: InheritableThreadLocal[SimbaContext] = new InheritableThreadLocal[SimbaContext]

  @transient private val instantiatedContext = new AtomicReference[SimbaContext]()

  def getOrCreate(sparkContext: SparkContext): SimbaContext = {
    val ctx = activeContext.get()
    if (ctx != null && !ctx.sparkContext.isStopped) {
      return ctx
    }

    synchronized {
      val ctx = instantiatedContext.get()
      if (ctx == null || ctx.sparkContext.isStopped) {
        new SimbaContext(sparkContext)
      } else {
        ctx
      }
    }
  }

  private[simba] def clearInstantiatedContext(): Unit = {
    instantiatedContext.set(null)
  }

  private[simba] def setInstantiatedContext(simbaContext: SimbaContext): Unit = {
    synchronized {
      val ctx = instantiatedContext.get()
      if (ctx == null || ctx.sparkContext.isStopped) {
        instantiatedContext.set(simbaContext)
      }
    }
  }

  private[simba] def getInstantiatedContextOption: Option[SimbaContext] = {
    Option(instantiatedContext.get())
  }

  def setActive(simbaContext: SimbaContext): Unit = {
    activeContext.set(simbaContext)
  }

  def clearActive(): Unit = {
    activeContext.remove()
  }

  private[simba] def getActive: Option[SimbaContext] = {
    Option(activeContext.get())
  }
}