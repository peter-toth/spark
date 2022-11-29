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

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.hive.test.TestHive
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.test.SQLTestUtils

class OpenCSVSerdeSuite extends QueryTest with SQLTestUtils with TestHiveSingleton {
  test("CDPD-17457: test read and write") {
    val tableName = "testtable"

    withTable(tableName) {
      sql(
        s"""CREATE TABLE $tableName (
           |  c1 byte,
           |  c2 short,
           |  c3 int,
           |  c4 long,
           |  c5 float,
           |  c6 double,
           |  c7 decimal(10, 5),
           |  c8 boolean,
           |  c9 date,
           |  c10 timestamp,
           |  c11 string,
           |  c12 binary)
           |ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
           |WITH SERDEPROPERTIES (
           |  'escapeChar'='\\\\',
           |  'quoteChar'='"',
           |  'separatorChar'=',')
           |STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
           |OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
           |""".stripMargin)

      sql(
        s"""INSERT INTO $tableName VALUES (
           |1,
           |2,
           |3,
           |4,
           |5.5,
           |6.6,
           |7.7,
           |true,
           |CAST('2020-01-01' AS date),
           |CAST('2020-01-02 01:02:03' AS timestamp),
           |'abc',
           |CAST('def' AS binary))
           |""".stripMargin)

      val df = sql(s"SELECT * FROM $tableName")
      checkAnswer(df, Row(
        1,
        2,
        3,
        4,
        5.5,
        6.6,
        7.7,
        true,
        Date.valueOf("2020-01-01"),
        Timestamp.valueOf("2020-01-02 01:02:03"),
        "abc",
        "def".getBytes
      ))

      val identifier = TableIdentifier(tableName)
      val location = spark.sessionState.catalog.getTableMetadata(identifier).location.toString
      val extTableName = s"${tableName}_external"

      withTable(extTableName) {
        sql(
          s"""CREATE EXTERNAL TABLE $extTableName (s STRING)
             |STORED AS textfile
             |LOCATION '$location'
             |""".stripMargin)

        val textDF = sql(s"SELECT * FROM $extTableName")
        // expected string is generated using Hive 3.1.3000.7.2.1.0-327
        checkAnswer(textDF, Row("\"1\",\"2\",\"3\",\"4\",\"5.5\",\"6.6\",\"7.7\",\"TRUE\"," +
          "\"2020-01-01\",\"2020-01-02 01:02:03\",\"abc\",\"def\""))
      }
    }
  }

  test("CDPD-47129: test empty CSV fields") {
    val tableName = "testemptyfieldstable"

    withTable(tableName) {
      sql(
        s"""CREATE TABLE $tableName (
           |  c1 byte,
           |  c2 short,
           |  c3 int,
           |  c4 long,
           |  c5 float,
           |  c6 double,
           |  c7 decimal(10, 5),
           |  c8 boolean,
           |  c9 date,
           |  c10 timestamp,
           |  c11 string,
           |  c12 binary)
           |ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
           |WITH SERDEPROPERTIES (
           |  'escapeChar'='\\\\',
           |  'quoteChar'='"',
           |  'separatorChar'=',')
           |STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
           |OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
           |""".stripMargin)

      val testData1 = TestHive.getHiveFile("data/files/empty_fields.csv").toURI
      sql(s"LOAD DATA INPATH '$testData1' INTO TABLE $tableName")
      val df = sql(s"SELECT * FROM $tableName")
      val date = Date.valueOf("2020-01-01")
      val ts = Timestamp.valueOf("2020-01-02 01:02:03")
      checkAnswer(df,
        Row(null, 2, 3, 4, 5.5, 6.6, 7.7, true, date, ts, "abc", "def".getBytes) ::
        Row(1, null, 3, 4, 5.5, 6.6, 7.7, true, date, ts, "abc", "def".getBytes) ::
        Row(1, 2, null, 4, 5.5, 6.6, 7.7, true, date, ts, "abc", "def".getBytes) ::
        Row(1, 2, 3, null, 5.5, 6.6, 7.7, true, date, ts, "abc", "def".getBytes) ::
        Row(1, 2, 3, 4, null, 6.6, 7.7, true, date, ts, "abc", "def".getBytes) ::
        Row(1, 2, 3, 4, 5.5, null, 7.7, true, date, ts, "abc", "def".getBytes) ::
        Row(1, 2, 3, 4, 5.5, 6.6, null, true, date, ts, "abc", "def".getBytes) ::
        Row(1, 2, 3, 4, 5.5, 6.6, 7.7, null, date, ts, "abc", "def".getBytes) ::
        Row(1, 2, 3, 4, 5.5, 6.6, 7.7, true, null, ts, "abc", "def".getBytes) ::
        Row(1, 2, 3, 4, 5.5, 6.6, 7.7, true, date, null, "abc", "def".getBytes) ::
        Row(1, 2, 3, 4, 5.5, 6.6, 7.7, true, date, ts, null, "def".getBytes) ::
        Row(1, 2, 3, 4, 5.5, 6.6, 7.7, true, date, ts, "abc", null) :: Nil)
    }
  }

  test("CDPD-47129: test writing and reading of NULLs") {
    val tableName = "testnullstable"

    withTable(tableName) {
      sql(
        s"""CREATE TABLE $tableName (
           |  c1 byte,
           |  c2 short,
           |  c3 int,
           |  c4 long,
           |  c5 float,
           |  c6 double,
           |  c7 decimal(10, 5),
           |  c8 boolean,
           |  c9 date,
           |  c10 timestamp,
           |  c11 string,
           |  c12 binary)
           |ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
           |WITH SERDEPROPERTIES (
           |  'escapeChar'='\\\\',
           |  'quoteChar'='"',
           |  'separatorChar'=',')
           |STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
           |OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
           |""".stripMargin)
      sql(
        s"""INSERT INTO $tableName VALUES (
           |NULL,
           |NULL,
           |NULL,
           |NULL,
           |NULL,
           |NULL,
           |NULL,
           |NULL,
           |NULL,
           |NULL,
           |NULL,
           |NULL)
           |""".stripMargin)
      val df = sql(s"SELECT * FROM $tableName")
      checkAnswer(df, Row(
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null))
    }
  }
}
