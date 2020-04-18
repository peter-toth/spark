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

import java.sql.Date
import java.text.SimpleDateFormat
import java.time.{DateTimeException, LocalDate}
import java.util.TimeZone

import scala.util.Random

import org.scalatest.BeforeAndAfterEach

import org.apache.spark.sql.{QueryTest, RandomDataGenerator}
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.types.DateType

case class TestDate(original: String, expected: String)

class DateCompatibilitySuite extends QueryTest with SQLTestUtils with TestHiveSingleton
  with BeforeAndAfterEach {

  val mustHaveDates = Seq(
    TestDate("2075-01-01", "2075-01-01"),
    TestDate("2017-01-01", "2017-01-01"),
    TestDate("1970-01-01", "1970-01-01"),
    TestDate("1952-06-23", "1952-06-23"),
    TestDate("1883-11-18", "1883-11-18"),
    TestDate("1883-10-10", "1883-10-10"),
    TestDate("1582-10-18", "1582-10-18"),
    TestDate("1582-10-15", "1582-10-15"),
    TestDate("1582-10-14", "1582-10-24"), // historical quirk of non-existent Julian dates
    TestDate("1582-10-03", "1582-10-03"),
    TestDate("1200-01-01", "1200-01-01"),
    TestDate("0800-02-29", "0800-02-29"),
    TestDate("0300-02-29", "0300-03-01"), // does not exist in proleptic Gregorian Calendar
    TestDate("0003-01-01", "0003-01-01")
  )

  // also get some random Dates
  val rand = {
    val seed = System.currentTimeMillis()
    log.warn(s"Seed is $seed") // allow us to reproduce failures
    new Random(seed)
  }
  val dataGenerator = RandomDataGenerator.forType(
    dataType = DateType,
    nullable = false,
    rand = rand
  ).get
  val extraDateCandidates = (0 to 50).map { x =>
    dataGenerator().asInstanceOf[Date]
  }.filter { date =>
    // filter out any random dates that cannot be represented in the gregorian calendar
    if (date.getYear + 1900 <= 1582) {
      try {
        LocalDate.of(
          date.getYear + 1900,
          date.getMonth + 1,
          date.getDate
        )
        true
      } catch {
        case d: DateTimeException =>
          logWarning(d.getMessage)
          false
      }
    } else {
      true
    }
  }

  val formatter = new SimpleDateFormat("yyyy-MM-dd")
  val extraDates = extraDateCandidates.map { date =>
    val str = formatter.format(date)
    TestDate(str, str)
  }

  val dates = mustHaveDates ++ extraDates

  val timezones = Seq(
    "UTC",
    "-08:00",
    "+01:00",
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

  timezones.foreach { tzString =>
    test(s"DATEs with parquet in timezone $tzString") {
      TimeZone.setDefault(TimeZone.getTimeZone(tzString))
      testDates("parquet", Some("spark.sql.hive.convertMetastoreParquet"))
    }
    test(s"DATEs with ORC in timezone $tzString") {
      TimeZone.setDefault(TimeZone.getTimeZone(tzString))
      testDates("orc", Some("spark.sql.hive.convertMetastoreOrc"))
    }
    test(s"DATEs with TEXTFILE in timezone $tzString") {
      TimeZone.setDefault(TimeZone.getTimeZone(tzString))
      testDates("textfile", None)
    }
  }

  private def testDates(format: String, configKeyOpt: Option[String]): Unit = {
    import spark.implicits._

    val dateStrings = dates.map(td => s"${td.expected},${td.original}").toDS()

    // The CSV date parser is far more lenient than CAST (CSV allows through 0300-02-29,
    // for example, whereas CAST does not). So therefore, use the CSV parser
    val df = spark.read.schema("expected STRING, dt DATE").csv(dateStrings)
    val tableName = s"testtable_$format"

    val checkQuery = s"""
      select *
      from $tableName
      where expected != CAST(dt as STRING)
    """

    // To keep the code the same for formats that don't have a convertMetaStoreXXX
    // setting, just use spark.sql.hive.convertMetastoreParquet when there is no key.
    // For the TEXTFILE  format, we will set the spark.sql.hive.convertMetastoreParquet config,
    // even though it has no impact on behavior of that file format.
    val configKey = configKeyOpt.getOrElse("spark.sql.hive.convertMetastoreParquet")
    withTable(tableName) {
      withSQLConf(configKey -> "true") {
        sql(s"create table $tableName (expected STRING, dt DATE) stored as $format")
        df.createOrReplaceTempView("df")
        // insert into the table using hive-exec
        withSQLConf(configKey -> "false") {
          sql(s"insert into $tableName select * from df")
          assert(sql(s"select * from $tableName").count == dates.size)
          assert(sql(checkQuery).count == 0)
        }
        assert(sql(checkQuery).count == 0)
      }
    }
  }
}
