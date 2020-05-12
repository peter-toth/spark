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

import java.time.ZoneOffset
import java.util.TimeZone

import org.apache.hadoop.hive.common.`type`.{Date => HiveDate, Timestamp => HiveTimestamp}

import org.apache.spark.sql.catalyst.util.DateTimeConstants._
import org.apache.spark.sql.catalyst.util.DateTimeUtils

object HiveDateTimeUtils {

  /**
   * Returns the number of days since epoch from HiveDate.
   */
  def fromHiveDate(date: HiveDate): DateTimeUtils.SQLDate = {
    val convertedMicros = DateTimeUtils.convertTz(date.toEpochMilli * 1000,
      TimeZone.getDefault.toZoneId, ZoneOffset.UTC)
    val r = DateTimeUtils.millisToDays(convertedMicros / 1000)
    r
  }

  def toHiveDate(daysSinceEpoch: DateTimeUtils.SQLDate): HiveDate = {
    val micros = DateTimeUtils.daysToMillis(daysSinceEpoch) * 1000
    val convertedMicros = DateTimeUtils.convertTz(micros, ZoneOffset.UTC,
      TimeZone.getDefault.toZoneId)
    val r = HiveDate.ofEpochMilli(convertedMicros / 1000)
    r
  }

  /**
   * Returns a HiveTimestamp from number of micros since epoch.
   */
  def toHiveTimestamp(us: DateTimeUtils.SQLTimestamp): HiveTimestamp = {
    // setNanos() will overwrite the millisecond part, so the milliseconds should be
    // cut off at seconds
    val usToDefault = DateTimeUtils.convertTz(us, ZoneOffset.UTC, TimeZone.getDefault.toZoneId)
    var seconds = usToDefault / MICROS_PER_SECOND
    var micros = usToDefault % MICROS_PER_SECOND
    // setNanos() can not accept negative value
    if (micros < 0) {
      micros += MICROS_PER_SECOND
      seconds -= 1
    }
    HiveTimestamp.ofEpochSecond(seconds, micros.toInt * 1000)
  }

  /**
   * Returns the number of micros since epoch from HiveTimestamp.
   */
  def fromHiveTimestamp(t: HiveTimestamp): DateTimeUtils.SQLTimestamp = {
    val jTimestamp = t.toSqlTimestamp
    val utcJTs = DateTimeUtils.fromJavaTimestamp(jTimestamp)
    DateTimeUtils.convertTz(utcJTs, TimeZone.getDefault.toZoneId, ZoneOffset.UTC)
  }
}
