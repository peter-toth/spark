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

package org.apache.spark.sql.catalyst.util

import java.sql.Timestamp
import java.time.{DateTimeException, LocalDateTime}
import java.util.TimeZone

import org.apache.spark.internal.Logging

/**
 * Helper functions for testing date and time functionality.
 */
object DateTimeTestUtils extends Logging {

  val ALL_TIMEZONES: Seq[TimeZone] = TimeZone.getAvailableIDs.toSeq.map(TimeZone.getTimeZone)

  val outstandingTimezonesIds: Seq[String] = Seq(
    "UTC",
    "PST",
    "CET",
    "Africa/Dakar",
    "America/Los_Angeles",
    "Antarctica/Vostok",
    "Asia/Hong_Kong",
    "Europe/Amsterdam")
  val outstandingTimezones: Seq[TimeZone] = outstandingTimezonesIds.map(TimeZone.getTimeZone)

  def withDefaultTimeZone[T](newDefaultTimeZone: TimeZone)(block: => T): T = {
    val originalDefaultTimeZone = TimeZone.getDefault
    try {
      DateTimeUtils.resetThreadLocals()
      TimeZone.setDefault(newDefaultTimeZone)
      block
    } finally {
      TimeZone.setDefault(originalDefaultTimeZone)
      DateTimeUtils.resetThreadLocals()
    }
  }

  def filterInvalidTimestamps(timestamps: Seq[Timestamp]): Seq[Timestamp] = {
    timestamps.filter { ts =>
      // filter out any random dates that cannot be represented in the gregorian calendar in the
      // current timezone.
      try {
        ts.toLocalDateTime
        true
      } catch {
        case d: DateTimeException =>
          logWarning(
            "%s (%s %04d-%02d-%02d %02d:%02d:%02d.%03d)"
              .format(
                d.getMessage,
                TimeZone.getDefault.getID,
                ts.getYear + 1900,
                ts.getMonth + 1,
                ts.getDate,
                ts.getHours,
                ts.getMinutes,
                ts.getSeconds,
                ts.getNanos
              )
          )
          false
      }
    }
  }
}
