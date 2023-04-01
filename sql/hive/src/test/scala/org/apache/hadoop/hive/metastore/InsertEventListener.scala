/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore


import java.io.{BufferedWriter, File, FileOutputStream, OutputStreamWriter}
import java.util.concurrent.atomic.AtomicInteger

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hadoop.hive.metastore.events.{AddPartitionEvent, CreateTableEvent, DropTableEvent, InsertEvent}

import org.apache.spark.sql.SparkSession


class InsertEventListener(config: Configuration) extends MetaStoreEventListener(config) {
  private var listenerEnabled: Boolean = false
  private var assertCounterOnTestTableDrop = false

  private def resultFile: File = {
    val scratchDir = new File(SparkSession.getActiveSession.get.sparkContext.
      hadoopConfiguration.get(ConfVars.SCRATCHDIR.varname).substring("file:".length))
    scratchDir.mkdir()
    val resultFile = config.get(InsertEventListener.keyForResultFileName)
    new java.io.File(scratchDir, resultFile)
  }

  override def onCreateTable(tableEvent: CreateTableEvent): Unit = {
    if (tableEvent.getTable.getTableName.equalsIgnoreCase(InsertEventListener.disableListener)) {
      listenerEnabled = false
    } else if (tableEvent.getTable.getTableName.equalsIgnoreCase(
      InsertEventListener.enableListener)) {
      listenerEnabled = true
    }
    if (listenerEnabled) {
      assertCounterOnTestTableDrop = true
      InsertEventListener.counter.set(0)
      val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(resultFile)))
      writer.write("0")
      writer.close()
    }
  }

  override def onInsert(insertEvent: InsertEvent): Unit = if (listenerEnabled) {
    val invocCount = InsertEventListener.counter.incrementAndGet()
    val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(resultFile)))
    writer.write(String.valueOf(invocCount))
    writer.close()
  }

  override def onAddPartition(partitionEvent: AddPartitionEvent): Unit = if (listenerEnabled) {
    val invocCount = InsertEventListener.counter.incrementAndGet()
    val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(resultFile)))
    writer.write(String.valueOf(invocCount))
    writer.close()
  }

  override def onDropTable(tableEvent: DropTableEvent): Unit = {
    val isListenerTablesDrop = tableEvent.getTable.getTableName.equalsIgnoreCase(
      InsertEventListener.enableListener) || tableEvent.getTable.getTableName.equalsIgnoreCase(
      InsertEventListener.disableListener)

    if (listenerEnabled && !isListenerTablesDrop) {
      if (assertCounterOnTestTableDrop) {
        assert(InsertEventListener.counter.get() > 0)
        // re-assert again only when a new test table is created
        assertCounterOnTestTableDrop = false
      }
      InsertEventListener.counter.set(0)
      val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(resultFile)))
      writer.write("0")
      writer.close()
    }
  }
}

object InsertEventListener {
  // we are using a file to keep track of counters instead of in-memory atomic counter, because
  // the listener is invoked in a class loader which is different from the test class loader.
  // so the increments done in Listener are not seen by the test.
  val keyForResultFileName = "hive.metastore.dml.events.resultfile"
  val counter = new AtomicInteger(0)
  val resultFileName = "result"
  val disableListener = "disableListener"
  val enableListener = "enableListener"
}
