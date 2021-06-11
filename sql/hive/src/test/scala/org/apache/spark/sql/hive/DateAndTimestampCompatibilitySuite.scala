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

import scala.util.Random

import org.scalatest.Ignore

import org.apache.spark.sql.{QueryTest, RandomDataGenerator}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.util.{DateTimeTestUtils, DateTimeUtils}
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.LegacyBehaviorPolicy
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.types.TimestampType

@Ignore
class DateAndTimestampCompatibilitySuite
    extends QueryTest with SQLTestUtils with TestHiveSingleton {
  // Depending on timezone, format, if the writer is spark native writer, if the reader is spark
  // native reader and other configs (e.g. ORC implementation) we can specify the expected date or
  // timestamp.
  // Actually, we don't use spark native writer, spark native reader and other configs in this Spark
  // 3 version of the test suite, but those params are needed in Spark 2 version it to handle
  // special test cases.
  type Expected = (String, String, Boolean, Boolean, Map[String, String]) => Option[String]

  case class TestCase(
      originalDate: String,
      originalTime: String,
      expectedDate: Expected,
      expectedTimestamp: Expected)

  object TestCase {
    def apply(
        originalDate: String,
        originalTime: String,
        expectedDate: String,
        expectedTimestamp: String): TestCase =
      new TestCase(originalDate, originalTime, (_, _, _, _, _) => Some(expectedDate),
        (_, _, _, _, _) => Some(expectedTimestamp))
    def apply(
        originalDate: String,
        originalTime: String,
        expectedDate: String,
        expectedTimestamp: Expected): TestCase =
      new TestCase(originalDate, originalTime,
        (_, _, _, _, _) => Some(expectedDate), expectedTimestamp)
  }

  val mustHaveCases = Seq(
    // https://jira.cloudera.com/browse/CDPD-16037:
    // The timestamp 0001-01-01 00:00:00 is tricky when it comes to parquet and timezones "before"
    // UTC. We don't run this test case in those timezones because the
    // org.apache.hadoop.hive.common.type.Timestamp class in CDP Hive is bogus.
    // The source of the issue is that the above timestamp in GMT+1 is 0000-12-31 23:00.00 in UTC
    // and the localDateTime field in the Timestamp class contains that. But the toString method of
    // the Timestamp class is returns 0001-12-31 23:00:00.
    // NanoTimeUtils.getTimestamp adds more to the issue by calling
    // TimestampTZUtil.convertTimestampToZone which uses the string representation of the Timestamp
    // class and converts the timestamp to 0002-01-01 00:00.00.
    TestCase("0001-01-01", "00:00:00", "0001-01-01",
      (timezone: String, format: String, _: Boolean, _: Boolean, _: Map[String, String]) =>
        if (format == "parquet" && gmtPlusTimezones.contains(timezone)) {
          None
        } else {
          Some("0001-01-01 00:00:00")
        }),

    TestCase("9999-12-31", "23:59:59.999", "9999-12-31", "9999-12-31 23:59:59.999"),
    TestCase("2075-01-01", "23:59:59.987654", "2075-01-01", "2075-01-01 23:59:59.987654"),
    TestCase("2017-01-01", "01:02:03.123456", "2017-01-01", "2017-01-01 01:02:03.123456"),
    TestCase("1970-01-01", "00:00:00.000", "1970-01-01", "1970-01-01 00:00:00"),
    TestCase("1952-06-23", "00:00:00.000", "1952-06-23", "1952-06-23 00:00:00"),
    TestCase("1883-11-18", "00:00:00.000", "1883-11-18", "1883-11-18 00:00:00"),
    TestCase("1883-10-10", "00:00:00.000", "1883-10-10", "1883-10-10 00:00:00"),
    TestCase("1582-10-18", "00:00:00.000", "1582-10-18", "1582-10-18 00:00:00"),

    // https://jira.cloudera.com/browse/CDPD-16388:
    // The timestamp 1582-10-15 00:00:00 is tricky when it comes to parquet and timezones "before"
    // UTC. We don't run this test case in those timezones because the conversion that is defined
    // in CDP Hive's NanoTimeUtils.getNanoTime and NanoTimeUtils.getTimestamp to store a timestamp
    // into INT96 is bogus.
    // The source of the issue is that NanoTimeUtils.getNanoTime first converts the timestamp into
    // local time in UTC (e.g. to 1582-10-14 23:40:28 from 1582-10-15 00:00:00 Europe/Amsterdam) and
    // the date part (1582-10-14) is then fed into a JDateTime. As that date doesn't exist in hybrid
    // calendar so it is shifted +10 days (standard way to deal with non-existing hybrid calendar
    // dates) and the date part becomes 1582-10-24. Since NanoTimeUtils.getTimestamp doesn't do the
    // -10 days shift (it simply can't know when to do that) this test case in those timezones would
    // fail even if just write the timestamp with hive-exec writer and then read it back with
    // hive-exec reader.
    TestCase("1582-10-15", "00:00:00.000", "1582-10-15",
      (timezone: String, format: String, _: Boolean, _: Boolean, _: Map[String, String]) =>
        if (format == "parquet" && gmtPlusTimezones.contains(timezone)) {
          None
        } else {
          Some("1582-10-15 00:00:00")
        }),

    // The date 1582-10-14 doesn't exist in the hybrid calendar and so it cannot be represented
    // with the legacy Java date/time classes. The legacy behavior is to shift the date with +10
    // days so we expect 1582-10-24.
    // Legacy behaviour requires explicit timezone to be specified, please see the details at test
    // case: 0300-02-29.
    // We run this test case only in UTC as we specify the timezone and the expected timestamp would
    // be tricky in other zones.
    TestCase("1582-10-14", "00:00:00.000Z", "1582-10-24",
      (timezone: String, _: String, _: Boolean, _: Boolean, _: Map[String, String]) =>
        if (timezone == "UTC") {
          Some("1582-10-24 00:00:00")
        } else {
          None
        }),
    // If we don't specify the timezone ('Z') then timestamp parsing falls back to new mode where
    // the non-existing timestamp date 1582-10-14 is shifted to the earliest existing one.
    // We don't run this test case with parquet and timezones "before" UTC due to
    // https://jira.cloudera.com/browse/CDPD-16388
    TestCase("1582-10-14", "00:00:00.000", "1582-10-24",
      (timezone: String, format: String, _: Boolean, _: Boolean, _: Map[String, String]) =>
        if (format == "parquet" && gmtPlusTimezones.contains(timezone)) {
          None
        } else {
          Some("1582-10-15 00:00:00")
        }),
    TestCase("1582-10-03", "00:00:00.000", "1582-10-03", "1582-10-03 00:00:00"),
    TestCase("1200-01-01", "00:00:00.000", "1200-01-01", "1200-01-01 00:00:00"),
    TestCase("0800-02-29", "00:00:00.000", "0800-02-29", "0800-02-29 00:00:00"),

    // The date 0300-02-29 doesn't exist in proleptic Gregorian Calendar so we need a trick here and
    // use lenient CSV parsing with the legacy parser. This combination (CSV + legacy) requires
    // explicit timezone (e.g. 'Z') to be specified. (Please see "....SSSXXX" pattern in
    // CSVOptions.timestampFormat for details.)
    // We run this test case only in UTC as we specify the timezone and the expected timestamp would
    // be tricky in other zones.
    TestCase("0300-02-29", "00:00:00.000Z", "0300-03-01",
      (timezone: String, _: String, _: Boolean, _: Boolean, _: Map[String, String]) =>
        if (timezone == "UTC") {
          Some("0300-03-01 00:00:00")
        } else {
          None
        }),
    TestCase("0003-01-01", "00:00:00.000", "0003-01-01", "0003-01-01 00:00:00"),

    // Close to DST switch (02:00:00 -> 03:00:00) in Europe/Amsterdam
    TestCase("2020-03-29", "01:18:35.845", "2020-03-29", "2020-03-29 01:18:35.845"),
    TestCase("2020-03-29", "02:18:35.845", "2020-03-29",
      (timezone: String, _: String, _: Boolean, _: Boolean, _: Map[String, String]) =>
        if (timezone == "Europe/Amsterdam") {
          Some("2020-03-29 03:18:35.845")
        } else {
          Some("2020-03-29 02:18:35.845")
        }),
    TestCase("2020-03-29", "03:18:35.845", "2020-03-29", "2020-03-29 03:18:35.845"),
    TestCase("2020-03-29", "04:18:35.845", "2020-03-29", "2020-03-29 04:18:35.845"),

    // Close to DST switch (03:00:00 -> 02:00:00) in Europe/Amsterdam
    TestCase("2020-10-25", "01:18:35.845", "2020-10-25", "2020-10-25 01:18:35.845"),
    TestCase("2020-10-25", "02:18:35.845", "2020-10-25", "2020-10-25 02:18:35.845"),
    TestCase("2020-10-25", "03:18:35.845", "2020-10-25", "2020-10-25 03:18:35.845"),
    TestCase("2020-10-25", "04:18:35.845", "2020-10-25", "2020-10-25 04:18:35.845"),

    // Far future close to DST switch (02:00:00 -> 03:00:00) in Europe/Amsterdam
    TestCase("8307-03-31", "01:20:58.928", "8307-03-31", "8307-03-31 01:20:58.928"),
    TestCase("8307-03-31", "02:20:58.928", "8307-03-31",
      (timezone: String, format: String, _: Boolean, _: Boolean, _: Map[String, String]) =>
        if (timezone == "Europe/Amsterdam") {
          if (format == "orc") {
            // https://jira.cloudera.com/browse/CDPD-16133:
            // Far future close to DST switch cases fail with ORC
            // Some("8307-03-31 03:20:58.928")
            None
          } else {
            Some("8307-03-31 03:20:58.928")
          }
        } else {
          Some("8307-03-31 02:20:58.928")
        }),
    TestCase("8307-03-31", "03:20:58.928", "8307-03-31",
      (timezone: String, format: String, _: Boolean, _: Boolean, _: Map[String, String]) =>
        if (timezone == "Europe/Amsterdam" && format == "orc") {
          // https://jira.cloudera.com/browse/CDPD-16133:
          // Far future close to DST switch cases fail with ORC
          // Some("8307-03-31 03:20:58.928")
          None
        } else {
          Some("8307-03-31 03:20:58.928")
        }),
    TestCase("8307-03-31", "04:20:58.928", "8307-03-31", "8307-03-31 04:20:58.928"),

    // The timestamp 0500-02-28 22:24:59.808 is tricky in some of the time zones. Without the
    // `SQLConf.PARQUET_INT96_TIMESTAMP_CDPHIVE3_COMPATIBILITY_IN_WRITE_ENABLED` flag and the new
    // way of conversion in `DateTimeUtils.fromCDPHive3CompatibleJulianDay` and
    // `DateTimeUtils.toCDPHive3CompatibleJulianDay` it returns
    // 0500-03-01 22:24:59.808 in GMT-08:00.
    TestCase("0500-02-28", "22:24:59.808", "0500-02-28", "0500-02-28 22:24:59.808"),

    // The following test cases are taken from CDPD-14906

    TestCase("2075-01-01", "21:19:59.64", "2075-01-01", "2075-01-01 21:19:59.64"),
    TestCase("2017-01-01", "06:47:16.845", "2017-01-01", "2017-01-01 06:47:16.845"),
    TestCase("1991-04-07", "03:46:57.715", "1991-04-07", "1991-04-07 03:46:57.715"),
    TestCase("1970-01-01", "00:00:00", "1970-01-01", "1970-01-01 00:00:00"),
    TestCase("1969-04-27", "03:53:06.2", "1969-04-27", "1969-04-27 03:53:06.2"),
    TestCase("1952-06-23", "04:53:48.914", "1952-06-23", "1952-06-23 04:53:48.914"),

    // On November 18, 1883, precisely at noon, North American railroads switched to a new
    // standard time system for rail operations
    TestCase("1883-11-18", "13:51:12.302", "1883-11-18", "1883-11-18 13:51:12.302"),
    TestCase("1883-10-10", "02:45:21.926", "1883-10-10", "1883-10-10 02:45:21.926"),

    TestCase("1582-10-18", "01:28:14.915", "1582-10-18", "1582-10-18 01:28:14.915"),

    // historical quirk of non-existent Julian dates
    TestCase("1582-10-14", "04:57:19.513Z", "1582-10-24",
      (timezone: String, _: String, _: Boolean, _: Boolean, _: Map[String, String]) =>
        if (timezone == "UTC") {
          Some("1582-10-24 04:57:19.513")
        } else {
          None
        }),

    TestCase("1582-10-03", "11:39:59.22", "1582-10-03", "1582-10-03 11:39:59.22"),
    TestCase("1281-12-25", "08:19:18.219", "1281-12-25", "1281-12-25 08:19:18.219"),
    TestCase("1200-01-01", "02:46:40.129", "1200-01-01", "1200-01-01 02:46:40.129"),
    TestCase("0800-02-29", "18:55:35.478", "0800-02-29", "0800-02-29 18:55:35.478"),

    // does not exist in proleptic Gregorian Calendar
    TestCase("0300-02-29", "21:08:10.958Z", "0300-03-01",
      (timezone: String, _: String, _: Boolean, _: Boolean, _: Map[String, String]) =>
        if (timezone == "UTC") {
          Some("0300-03-01 21:08:10.958")
        } else {
          None
        }),

    // these timestamps were the basis of one of the complaints in ENGESC-3058
    TestCase("0001-01-02", "20:07:01.57", "0001-01-02", "0001-01-02 20:07:01.57"),
    TestCase("0001-01-01", "15:26:38.592", "0001-01-01", "0001-01-01 15:26:38.592")
  )

  val rand = {
    val seed = System.currentTimeMillis()
    log.warn(s"Seed is $seed") // allow us to reproduce failures
    new Random(seed)
  }
  val dataGenerator = RandomDataGenerator.forType(
    dataType = TimestampType,
    nullable = false,
    rand = rand
  ).get
  // RandomDataGenerator in Spark 3 returns dates/timestamps that do exist in both hybrid calendar
  // and proleptic Gregorian Calendar.
  val randomTimestamps = Seq.fill(50)(dataGenerator().asInstanceOf[Timestamp])

  val sqlTypes = Seq(
    "DATE",
    "TIMESTAMP"
  )
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
  // This list of GMT+x timezones is not entirely correct as there were changes in some of the zones
  // above e.g. "Antarctica/Vostok" changed from UTC to UTC+06:00 at 1957 Mon, 16 Dec, 00:00
  // (https://www.timeanddate.com/time/zone/antarctica/vostok-station)
  val gmtPlusTimezones = Seq(
    "GMT+01:00",
    "Antarctica/Vostok",
    "Asia/Hong_Kong",
    "Europe/Amsterdam",
    "Pacific/Auckland"
  )
  val formatsAndConfigs = Seq(
    ("parquet", Some(HiveUtils.CONVERT_METASTORE_PARQUET.key),
      Some(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> Seq("true", "false"))),
    ("orc", Some(HiveUtils.CONVERT_METASTORE_ORC.key),
      Some(SQLConf.ORC_IMPLEMENTATION.key -> Seq("hive", "native"))),
    ("textfile", None, None)
  )

  sqlTypes.foreach { sqlType =>
    timezones.foreach { timezone =>
      // Do the string transformation of the random timestamps in the tested timezone to avoid DST
      // switch problem with expected timestamp.
      DateTimeTestUtils.withDefaultTimeZone(DateTimeUtils.getZoneId(timezone)) {
        val randomCases = randomTimestamps.map { ts =>
          val ldt = ts.toLocalDateTime

          val dateStr = ldt.toLocalDate.toString
          val timeStr = ldt.toLocalTime.toString

          // remove ending 0 milliseconds if needed
          val expectedTimeStr = if (timeStr.contains('.')) {
            timeStr.reverse.dropWhile(_ == '0').reverse
          } else {
            // add 00 seconds it needed
            if (timeStr.count(_ == ':') < 2) {
              timeStr + ":00"
            } else {
              timeStr
            }
          }

          TestCase(dateStr, timeStr, (_, _, _, _, _) =>
            if (ldt.getYear > 9999) {
              // RandomDataGenerator can return 10000-01-01 that can't be handled by Spark
              None
            } else {
              Some(dateStr)
            }, (_, format, _, _, _) =>
            if (format == "orc" && ldt.getYear > 5000) {
              // https://jira.cloudera.com/browse/CDPD-16133:
              // Far future close to DST switch cases fail with ORC
              None
            } else if (format == "parquet" && ldt.getYear == 1) {
              // https://jira.cloudera.com/browse/CDPD-16037:
              None
            } else if (ldt.getYear > 9999) {
              // RandomDataGenerator can return 10000-01-01 that can't be handled by Spark
              None
            } else {
              Some(dateStr + " " + expectedTimeStr)
            })
        }

        formatsAndConfigs.foreach { case (format, formatConfigKey, otherConfigs) =>
          test(s"${sqlType}s with $format in timezone $timezone") {
            testCases(sqlType, timezone, format, formatConfigKey, otherConfigs,
              mustHaveCases ++ randomCases)
          }
        }
      }
    }
  }

  private def check(tableName: String, format: String): Unit = {
    val checkQuery = s"""
        |SELECT original, cast(original AS STRING) AS casted, expected
        |FROM $tableName
        |WHERE !(cast(original AS STRING) <=> expected)
        |""".stripMargin

    checkAnswer(sql(checkQuery), Nil)

    if (format == "textfile") {
      val identifier = TableIdentifier(tableName)
      val location = spark.sessionState.catalog.getTableMetadata(identifier).location.toString
      val extTableName = s"${tableName}_external"

      withTable(extTableName) {
        sql(s"""
            |CREATE EXTERNAL TABLE $extTableName (original STRING, expected STRING)
            |STORED AS textfile
            |LOCATION '$location'
            |""".stripMargin)

        val checkQuery = s"""
            |SELECT original, expected
            |FROM $extTableName
            |WHERE !(original <=> expected)
            |""".stripMargin

        checkAnswer(sql(checkQuery), Nil)
      }
    }
  }

  private def withTestTable(
      sqlType: String,
      timezone: String,
      format: String,
      sparkWriter: Boolean,
      sparkReader: Boolean,
      otherConfigs: Map[String, String],
      cases: Seq[TestCase])(f: String => Unit): Unit = {
    import spark.implicits._

    withTempView("df") {
      val csvRows = cases.flatMap { c =>
        if (sqlType == "DATE") {
          c.expectedDate(timezone, format, sparkWriter, sparkReader, otherConfigs).map { ed =>
            s"${c.originalDate},$ed"
          }
        } else {
          c.expectedTimestamp(timezone, format, sparkWriter, sparkReader, otherConfigs).map { et =>
            s"${c.originalDate}T${c.originalTime},$et"
          }
        }
      }
      // The CSV date parser is far more lenient than CAST (CSV allows through 0300-02-29, for
      // example, whereas CAST does not). So therefore, use the CSV parser.
      spark.read.schema(s"original $sqlType, expected STRING").csv(csvRows.toDS())
        .createTempView("df")

      checkAnswer(sql("SELECT * FROM df WHERE expected IS null"), Nil)

      val tableName = s"testtable_$format"

      withTable(tableName) {
        sql(s"CREATE TABLE $tableName (original $sqlType, expected STRING) STORED AS $format")

        sql(s"INSERT INTO $tableName SELECT * FROM df")
        assert(sql(s"SELECT * FROM $tableName").count == csvRows.size)

        f(tableName)
      }
    }
  }

  private def testCasesWithConfiguration(
      sqlType: String,
      timezone: String,
      format: String,
      formatConfigKey: Option[String],
      formatConfigValue: Boolean,
      otherConfigs: Map[String, String],
      cases: Seq[TestCase]): Unit = {
    withSQLConf(formatConfigKey.map(_ -> formatConfigValue.toString).toSeq: _*) {
      withClue(s"spark writer: $formatConfigValue") {
        withTestTable(sqlType, timezone, format, formatConfigValue, formatConfigValue, otherConfigs,
          cases) { tableName =>
          withClue(s"spark reader: $formatConfigValue") {
            check(tableName, format)

            // if we are testing parquet (int96) timestamps and the file was written with spark
            // writer then the `org.apache.spark.cdpHive3CompatibleINT96` flag was placed in the
            // file metadata and we should be able to read it back with spark reader correctly
            // regardless the value of
            // `spark.cloudera.sql.parquet.int96Timestamp.cdpHive3CompatibilityInRead.enabled` and
            // `spark.sql.legacy.parquet.int96RebaseModeInRead`
            if (sqlType == "TIMESTAMP" && format == "parquet" && formatConfigValue) {
              withSQLConf(SQLConf.PARQUET_INT96_TIMESTAMP_CDPHIVE3_COMPATIBILITY_IN_READ_ENABLED.key
                -> "false") {
                withClue(s"${SQLConf.PARQUET_INT96_TIMESTAMP_CDPHIVE3_COMPATIBILITY_IN_READ_ENABLED
                  .key} = false") {
                  check(tableName, format)
                  testExceptionAndLegacyInt96RebaseModeInRead()
                }
              }
              testExceptionAndLegacyInt96RebaseModeInRead()

              def testExceptionAndLegacyInt96RebaseModeInRead() = {
                Seq(LegacyBehaviorPolicy.EXCEPTION, LegacyBehaviorPolicy.LEGACY).foreach {
                  int96rebaseModeInRead =>
                    withSQLConf(SQLConf.LEGACY_PARQUET_INT96_REBASE_MODE_IN_READ.key ->
                      int96rebaseModeInRead.toString) {
                      withClue(s"${SQLConf.LEGACY_PARQUET_INT96_REBASE_MODE_IN_READ.key} = " +
                        s"$int96rebaseModeInRead") {
                        check(tableName, format)
                      }
                    }
                }
              }
            }
          }
        }
        if (formatConfigKey.isDefined) {
          withTestTable(sqlType, timezone, format, formatConfigValue, !formatConfigValue,
            otherConfigs, cases) { tableName =>
            withSQLConf(formatConfigKey.map(_ -> (!formatConfigValue).toString).toSeq: _*) {
              withClue(s"spark reader: ${!formatConfigValue}") {
                check(tableName, format)
              }
            }
          }
        }
      }
    }
  }

  private def testCases(
      sqlType: String,
      timezone: String,
      format: String,
      formatConfigKey: Option[String],
      otherConfigs: Option[(String, Seq[String])],
      cases: Seq[TestCase]): Unit = {
    DateTimeTestUtils.withDefaultTimeZone(DateTimeUtils.getZoneId(timezone)) {
      withSQLConf(
        SQLConf.LEGACY_TIME_PARSER_POLICY.key -> LegacyBehaviorPolicy.LEGACY.toString,
        SQLConf.LEGACY_PARQUET_REBASE_MODE_IN_READ.key -> LegacyBehaviorPolicy.LEGACY.toString,
        SQLConf.LEGACY_PARQUET_REBASE_MODE_IN_WRITE.key -> LegacyBehaviorPolicy.LEGACY.toString,
        SQLConf.PARQUET_INT96_TIMESTAMP_CDPHIVE3_COMPATIBILITY_IN_READ_ENABLED.key -> "true",
        SQLConf.PARQUET_INT96_TIMESTAMP_CDPHIVE3_COMPATIBILITY_IN_WRITE_ENABLED.key -> "true",
        // TODO: this test suite doesn't test cases when session time zone is different to default
        // time zone
        SQLConf.SESSION_LOCAL_TIMEZONE.key -> timezone) {
        // Insert into the table using hive-exec writer and then read with both hive-exec and
        // built-in reader and vice versa, insert into the table using built-in writer and then read
        // with both built-in and hive-exec reader. In case of TEXTFILE the only option is the
        // hive-exec writer.
        (if (formatConfigKey.isDefined) {
          Seq(false, true)
        } else {
          Seq(false)
        }).foreach { formatConfigValue =>
          otherConfigs match {
            case Some((otherConfigKey, otherConfigValues)) =>
              otherConfigValues.foreach { otherConfigValue =>
                withSQLConf(otherConfigKey -> otherConfigValue) {
                  withClue(s"$otherConfigKey: $otherConfigValue") {
                    testCasesWithConfiguration(sqlType, timezone, format, formatConfigKey,
                      formatConfigValue, Map(otherConfigKey -> otherConfigValue), cases)
                  }
                }
              }
            case None =>
              testCasesWithConfiguration(sqlType, timezone, format, formatConfigKey,
                formatConfigValue, Map.empty, cases)
          }
        }
      }
    }
  }
}
