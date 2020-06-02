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

package org.apache.spark.sql.hive.util

import java.time.{LocalDate, LocalDateTime, LocalTime, ZoneOffset}
import java.util.TimeZone

import org.apache.hadoop.hive.common.`type`.{Date => HiveDate, Timestamp => HiveTimestamp}

import org.apache.spark.sql.catalyst.util.DateTimeUtils._
import org.apache.spark.sql.catalyst.util.RebaseDateTime
import org.apache.spark.sql.internal.SQLConf

object HiveDateTimeUtils {

  private final val julianEndSQLDate = RebaseDateTime.julianEndDate.toEpochDay.toInt
  private final val gregorianStartSQLDate = RebaseDateTime.gregorianStartDate.toEpochDay.toInt

  /**
   * Returns the number of days since epoch in Proleptic Gregorian Calendar from the Hive Date.
   */
  def fromHiveDate(date: HiveDate): SQLDate = {
    date.toEpochDay
  }

  /**
   * Returns the Hive Date from the number of days since epoch in Proleptic Gregorian Calendar.
   */
  def toHiveDate(days: SQLDate): HiveDate = {
    val shiftedDays =
      if (julianEndSQLDate < days && days < gregorianStartSQLDate) {
        gregorianStartSQLDate
      } else {
        days
      }
    HiveDate.ofEpochDay(shiftedDays)
  }

  /**
   * Returns the number of microseconds since epoch in Proleptic Gregorian Calendar from the
   * Hive Timestamp.
   */
  def fromHiveTimestamp(timestamp: HiveTimestamp): SQLTimestamp = {
    val ldt =
      LocalDateTime.ofEpochSecond(timestamp.toEpochSecond, timestamp.getNanos, ZoneOffset.UTC)
    val instant = ldt.atZone(getZoneId(SQLConf.get.sessionLocalTimeZone)).toInstant
    instantToMicros(instant)
  }

  /**
   * Returns the Hive Timestamp from the number of microseconds since epoch in Proleptic Gregorian
   * Calendar.
   */
  def toHiveTimestamp(micros: SQLTimestamp): HiveTimestamp = {
    val instant = microsToInstant(micros)
    var ldt = LocalDateTime.ofInstant(instant, getZoneId(SQLConf.get.sessionLocalTimeZone))
    if (ldt.isAfter(RebaseDateTime.julianEndTs) && ldt.isBefore(RebaseDateTime.gregorianStartTs)) {
      ldt = LocalDateTime.of(RebaseDateTime.gregorianStartDate, ldt.toLocalTime)
    }
    HiveTimestamp.ofEpochSecond(ldt.toEpochSecond(ZoneOffset.UTC), ldt.getNano)
  }
}
