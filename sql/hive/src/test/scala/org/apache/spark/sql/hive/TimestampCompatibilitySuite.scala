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

package org.apache.spark.sql.hive

import java.sql.Timestamp
import java.time.{DateTimeException, LocalDateTime}
import java.util.TimeZone

import scala.util.Random

import org.scalatest.BeforeAndAfterEach

import org.apache.spark.sql.{Dataset, QueryTest, RandomDataGenerator, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.util.DateTimeTestUtils
import org.apache.spark.sql.hive.HiveUtils.{CONVERT_METASTORE_ORC, CONVERT_METASTORE_PARQUET}
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.types.TimestampType

case class TestTimestamp(original: String, expected: String)

object TestTimestamp {
  def apply(stringValue: String): TestTimestamp = {
    TestTimestamp(stringValue, stringValue)
  }
}

class TimestampCompatibilitySuite extends QueryTest with SQLTestUtils with TestHiveSingleton
  with BeforeAndAfterEach {

  val mustHaveTimestamps = Seq(
    TestTimestamp("2075-01-01 21:19:59.64"),
    TestTimestamp("2017-01-01 06:47:16.845"),
    TestTimestamp("1991-04-07 03:46:57.715"),
    TestTimestamp("1970-01-01 00:00:00"),
    TestTimestamp("1969-04-27 03:53:06.2"),
    TestTimestamp("1952-06-23 04:53:48.914"),

    // On November 18, 1883, precisely at noon, North American railroads switched to a new
    // standard time system for rail operations
    TestTimestamp("1883-11-18 13:51:12.302"),
    TestTimestamp("1883-10-10 02:45:21.926"),

    TestTimestamp("1582-10-18 01:28:14.915"),

    // historical quirk of non-existent Julian dates
    TestTimestamp("1582-10-14 04:57:19.513", "1582-10-24 04:57:19.513"),

    TestTimestamp("1582-10-03 11:39:59.22"),
    TestTimestamp("1281-12-25 08:19:18.219"),
    TestTimestamp("1200-01-01 02:46:40.129"),
    TestTimestamp("0800-02-29 18:55:35.478"),

    // does not exist in proleptic Gregorian Calendar
    TestTimestamp("0300-02-29 21:08:10.958", "0300-03-01 21:08:10.958"),

    // these timestamps were the basis of one of the complaints in ENGESC-3058
    TestTimestamp("0001-01-02 20:07:01.57"),
    TestTimestamp("0001-01-01 15:26:38.592")
  )

  val seed = System.currentTimeMillis()
  logWarning(s"Seed is $seed") // allow us to reproduce failures

  // pre-generate all the java.sql.Timestamp objects.
  // We will need to filter and format these timestamps within the timezones in which they will
  // be tested, however, since, the individual components (year, month, day, hour, etc.)
  // will change depending on the time zone, and not all combination of
  // components exist in every time zone (e.g.,
  // 1993-10-03 02:21:17.749 exists in the America/Los_Angeles time zone, but it
  // doesn't exist in the Pacific/Auckland timezone due to daylight savings)
  val rand = new Random(seed)
  val dataGenerator = RandomDataGenerator.forType(
    dataType = TimestampType,
    nullable = false,
    rand = rand
  ).get

  val rawTimestamps = (0 to 50).map { x =>
    dataGenerator().asInstanceOf[Timestamp]
  }

  private def getRandomTimestamps(): Seq[TestTimestamp] = {
    val candidates = DateTimeTestUtils.filterInvalidTimestamps(rawTimestamps)
    candidates.map { ts =>
      // unfortunately, we can't use SimpleDateFormat, as it insists on adding trailing zeroes to
      // milliseconds, even with a single "S", which screws up comparisons because
      // CAST(ts as STRING) does not include trailing zeroes in milliseconds. We could use
      // date_format, rather than cast, in the query. However, the TEXTFILE data, which we examine
      // directly in checkTextfileStrings, also does not include trailing zeroes in milliseconds, so
      // we still must ensure no trailing zeroes in our expected value.
      val year = ts.getYear + 1900
      val month = ts.getMonth + 1
      val day = ts.getDate
      val hour = ts.getHours
      val minute = ts.getMinutes
      val second = ts.getSeconds
      var milli = ts.getNanos / 1000000

      // get rid of trailing zeroes in milli
      (0 to 1).foreach { _ =>
        if (milli % 10 == 0) {
          milli = milli / 10
        }
      }
      val part1 = f"$year%04d-$month%02d-$day%02d $hour%02d:$minute%02d:$second%02d"
      val part2 = if (milli > 0) {
        f".$milli%d"
      } else {
        ""
      }

      val str = part1 + part2
      TestTimestamp(str, str)
    }
  }

  val timezones = Seq(
    "UTC",
    "GMT-08:00",
    "GMT+01:00",
    "Africa/Dakar",
    "America/Los_Angeles",
    "Antarctica/Vostok",
    "Asia/Hong_Kong",
    "Europe/Amsterdam",
    "Pacific/Auckland"
  )
  val defaultTz = TimeZone.getDefault

  override def afterEach(): Unit = {
    TimeZone.setDefault(defaultTz)
  }

  timezones.foreach { tzId =>
    TimeZone.setDefault(TimeZone.getTimeZone(tzId))
    val timestamps = mustHaveTimestamps ++ getRandomTimestamps()

    test(s"TIMESTAMPs with parquet in timezone $tzId") {
      TimeZone.setDefault(TimeZone.getTimeZone(tzId))
      testTimestamps("parquet", tzId, timestamps, Some(CONVERT_METASTORE_PARQUET.key))
    }
    test(s"TIMESTAMPs with ORC in timezone $tzId") {
      TimeZone.setDefault(TimeZone.getTimeZone(tzId))
      testTimestamps("orc", tzId, timestamps, Some(CONVERT_METASTORE_ORC.key))
    }
    test(s"TIMESTAMPs with orc.impl=hive in timezone $tzId") {
      TimeZone.setDefault(TimeZone.getTimeZone(tzId))
      testOrcFileFormat(timestamps)
    }
    test(s"TIMESTAMPs with TEXTFILE in timezone $tzId") {
      TimeZone.setDefault(TimeZone.getTimeZone(tzId))
      testTimestamps("textfile", tzId, timestamps, None)
    }
  }

  test("Test microsecond precision for pre-Gregorian timestamp") {
    import spark.implicits._
    val ts = Timestamp.valueOf("0800-02-29 18:55:35.000789")
    val df = Seq(ts).toDF("ts")
    val tableName = s"testtable_orc_precision"
    df.createOrReplaceTempView("df")
    withTable(tableName) {
      withSQLConf(CONVERT_METASTORE_ORC.key -> "false") {
        sql(s"create table $tableName (ts TIMESTAMP) stored as orc")
        sql(s"insert into $tableName select * from df")
       checkAnswer(sql(s"select * from $tableName"), Row(ts))
      }
    }
  }

  private def getTimestampDf(timestamps: Seq[TestTimestamp]): Dataset[Row] = {
    import spark.implicits._
    val tsStrings = timestamps.map(td => s"${td.expected},${td.original}").toDS()

    // The CSV date parser is far more lenient than CAST (CSV allows through 0300-02-29,
    // for example, whereas CAST does not). So therefore, use the CSV parser
    spark.read.schema("expected STRING, ts TIMESTAMP").csv(tsStrings)
  }

  private def testTimestamps(
      format: String,
      tzId: String,
      timestamps: Seq[TestTimestamp],
      configKeyOpt: Option[String]): Unit = {

    val df = getTimestampDf(timestamps)
    val tableName = s"testtable_$format"

    // I am doing a negative check (find the count where things don't match) rather than
    // a positive check (find the count where things match) because for parquet
    // we don't know the exact match count ahead of time due to oddities in the way hive-exec
    // stores parquet timestamps.
    val checkQueryPrefix = s"""
      select *
      from $tableName
      where (expected != CAST(ts as STRING) or ts is null)
      """

    // To keep the code the same for formats that don't have a convertMetaStoreXXX
    // setting, just use spark.sql.hive.convertMetastoreParquet when there is no key.
    // For the TEXTFILE  format, we will set the spark.sql.hive.convertMetastoreParquet config,
    // even though it has no impact on behavior of that file format.
    val configKey = configKeyOpt.getOrElse(CONVERT_METASTORE_PARQUET.key)
    withTable(tableName) {
      withSQLConf(configKey -> "true") {
        sql(s"create table $tableName (expected STRING, ts TIMESTAMP) stored as $format")
        df.createOrReplaceTempView("df")
        // insert into the table using hive-exec
        withSQLConf(configKey -> "false") {
          sql(s"insert into $tableName select * from df")
          assert(sql(s"select * from $tableName").count == timestamps.size)

          // due to CDPD-15669, we should avoid checking timestamps with the date
          // 1582-10-15 for parquet tables in certain time zones
          val checkQuery = if ((tzId == "Pacific/Auckland" ||
              tzId == "Asia/Hong_Kong") &&
              format == "parquet") {
            s"$checkQueryPrefix and expected not like '1582-10-15 %'"
          } else {
            checkQueryPrefix
          }
          sql(checkQuery).show(100, false)
          assert(sql(checkQuery).count == 0)
          if (format == "textfile") {
            // In CDPD-14906, data was getting corrupted when stored, but frequently
            // de-corrupted on retrieval, so the data often looked OK to Spark. However, non-Spark
            // processors (like Hive) still saw corrupted data. Here, we can actually check that the
            // timestamps are being stored correctly.
            // We can take advantage of the fact that text files store data as readable strings.
            // Therefore, we can simply compare the two strings (ts and expected).
            checkTextfileStrings(tableName)
          }
        }
        val nativeQuery = if (format == "parquet" && tzId != "UTC") {
          // Due to CDPD-16035, the parquet SERDE shifts timestamps before the 20th
          // century in an way that is not compatible with Spark (or Hive 2), but only for
          // non-UTC time zones
          s"$checkQueryPrefix and ts >= '1900-01-01 00:00:00'"
        } else {
          checkQueryPrefix
        }
        sql(nativeQuery).show(100, false)
        assert(sql(nativeQuery).count == 0)
      }
    }
  }

  private def testOrcFileFormat(timestamps: Seq[TestTimestamp]): Unit = {
    val df = getTimestampDf(timestamps)
    val tableName = s"testtable_orcfileformat"

    val checkQuery = s"""
      select *
      from $tableName
      where ts is not null and expected = cast(ts as string)
      """

    withTable(tableName) {
      sql(s"create table $tableName (expected STRING, ts TIMESTAMP) stored as orc")
      df.createOrReplaceTempView("df")
      withSQLConf(CONVERT_METASTORE_ORC.key -> "true") {
        withSQLConf(SQLConf.ORC_IMPLEMENTATION.key -> "hive") {
          sql(s"insert into $tableName select * from df")
          assert(sql(s"select * from $tableName").count == timestamps.size)
          assert(sql(checkQuery).count == timestamps.size)
        }
        withSQLConf(SQLConf.ORC_IMPLEMENTATION.key -> "native") {
          assert(sql(checkQuery).count == timestamps.size)
        }
      }
    }
  }

  private def checkTextfileStrings(tableName: String): Unit = {
    val identifier = TableIdentifier(tableName)
    val location = spark.sessionState.catalog.getTableMetadata(identifier).location.toString
    val extTableName = s"${tableName}_external"
    withTable(extTableName) {
      sql(s"""
        create external table $extTableName
        (expected STRING, ts STRING)
        stored as TEXTFILE
        location '$location'
        """)
      val checkQuery = s"""
        select *
        from $extTableName
        where ts != expected
        """
      assert(sql(checkQuery).count == 0)
    }

  }
}
