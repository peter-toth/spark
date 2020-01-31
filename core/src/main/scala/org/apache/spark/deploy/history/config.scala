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

package org.apache.spark.deploy.history

import java.util.concurrent.TimeUnit

import org.apache.spark.internal.config.{ConfigBuilder, EVENT_LOG_ROLLING_MAX_FILE_SIZE}
import org.apache.spark.network.util.ByteUnit

private[spark] object config {

  val DEFAULT_LOG_DIR = "file:/tmp/spark-events"

  val EVENT_LOG_DIR = ConfigBuilder("spark.history.fs.logDirectory")
    .stringConf
    .createWithDefault(DEFAULT_LOG_DIR)

  val CLEANER_ENABLED = ConfigBuilder("spark.history.fs.cleaner.enabled")
    .booleanConf
    .createWithDefault(false)

  val CLEANER_INTERVAL_S = ConfigBuilder("spark.history.fs.cleaner.interval")
    .timeConf(TimeUnit.SECONDS)
    .createWithDefaultString("1d")

  val MAX_LOG_AGE_S = ConfigBuilder("spark.history.fs.cleaner.maxAge")
    .timeConf(TimeUnit.SECONDS)
    .createWithDefaultString("7d")

  val MAX_LOG_NUM = ConfigBuilder("spark.history.fs.cleaner.maxNum")
    .doc("The maximum number of log files in the event log directory.")
    .intConf
    .createWithDefault(Int.MaxValue)

  val LOCAL_STORE_DIR = ConfigBuilder("spark.history.store.path")
    .doc("Local directory where to cache application history information. By default this is " +
      "not set, meaning all history information will be kept in memory.")
    .stringConf
    .createOptional

  val MAX_LOCAL_DISK_USAGE = ConfigBuilder("spark.history.store.maxDiskUsage")
    .bytesConf(ByteUnit.BYTE)
    .createWithDefaultString("10g")

  val HISTORY_SERVER_UI_PORT = ConfigBuilder("spark.history.ui.port")
    .doc("Web UI port to bind Spark History Server")
    .intConf
    .createWithDefault(18080)

  val FAST_IN_PROGRESS_PARSING =
    ConfigBuilder("spark.history.fs.inProgressOptimization.enabled")
      .doc("Enable optimized handling of in-progress logs. This option may leave finished " +
        "applications that fail to rename their event logs listed as in-progress.")
      .booleanConf
      .createWithDefault(true)

  val END_EVENT_REPARSE_CHUNK_SIZE =
    ConfigBuilder("spark.history.fs.endEventReparseChunkSize")
      .doc("How many bytes to parse at the end of log files looking for the end event. " +
        "This is used to speed up generation of application listings by skipping unnecessary " +
        "parts of event log files. It can be disabled by setting this config to 0.")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("1m")

  private[spark] val EVENT_LOG_ROLLING_MAX_FILES_TO_RETAIN =
    ConfigBuilder("spark.history.fs.eventLog.rolling.maxFilesToRetain")
      .doc("The maximum number of event log files which will be retained as non-compacted. " +
        "By default, all event log files will be retained. Please set the configuration " +
        s"and ${EVENT_LOG_ROLLING_MAX_FILE_SIZE.key} accordingly if you want to control " +
        "the overall size of event log files.")
      .intConf
      .checkValue(_ > 0, "Max event log files to retain should be higher than 0.")
      .createWithDefault(Integer.MAX_VALUE)

  private[spark] val EVENT_LOG_COMPACTION_SCORE_THRESHOLD =
    ConfigBuilder("spark.history.fs.eventLog.rolling.compaction.score.threshold")
      .doc("The threshold score to determine whether it's good to do the compaction or not. " +
        "The compaction score is calculated in analyzing, and being compared to this value. " +
        "Compaction will proceed only when the score is higher than the threshold value.")
      .internal()
      .doubleConf
      .createWithDefault(0.7d)

  val DRIVER_LOG_CLEANER_ENABLED = ConfigBuilder("spark.history.fs.driverlog.cleaner.enabled")
    .fallbackConf(CLEANER_ENABLED)

  val DRIVER_LOG_CLEANER_INTERVAL = ConfigBuilder("spark.history.fs.driverlog.cleaner.interval")
    .fallbackConf(CLEANER_INTERVAL_S)

  val MAX_DRIVER_LOG_AGE_S = ConfigBuilder("spark.history.fs.driverlog.cleaner.maxAge")
    .fallbackConf(MAX_LOG_AGE_S)
}
