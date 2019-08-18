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

package org.apache.spark.sql.hive.execution

import java.io.File

import com.google.common.io.Files

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.catalyst.catalog.CatalogUtils
import org.apache.spark.sql.hive.HiveUtils
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SQLTestUtils

class HiveTranslationLayerSuite extends QueryTest with SQLTestUtils with TestHiveSingleton {
  import testImplicits._

  private def testRO(expectedMessage: String): Unit = {
    withSQLConf((SQLConf.CHECK_TRANSLATION_LAYER.key, "true")) {
      var message = intercept[AnalysisException] {
        sql("INSERT INTO transtbl PARTITION (code='b') select 1, 'a'")
      }.getMessage
      assert(message.contains(expectedMessage))

      // create some data for load
      val df = Seq.tabulate(10) { x => (x, ('a' + x).toChar.toString, "B") }
        .toDF("employeeID", "employeeName", "code")

      val tmpBase = Files.createTempDir()
      val tmp = new File(tmpBase, "inputData")
      tmp.deleteOnExit()
      df.write.save(tmp.getAbsolutePath)

      message = intercept[AnalysisException] {
        sql(s"LOAD DATA INPATH '${tmp.getAbsolutePath}' INTO TABLE transtbl")
      }.getMessage
      assert(message.contains(expectedMessage))

      message = intercept[AnalysisException] {
        sql("DROP TABLE transtbl")
      }.getMessage
      assert(message.contains(expectedMessage))

      message = intercept[AnalysisException] {
        sql("TRUNCATE TABLE transtbl")
      }.getMessage
      assert(message.contains(expectedMessage))

      message = intercept[AnalysisException] {
        sql("ALTER TABLE transtbl CHANGE employeeID employeeID2 INT")
      }.getMessage
      assert(message.contains(expectedMessage))

      message = intercept[AnalysisException] {
        sql("ALTER TABLE transtbl ADD COLUMNS (e INT)")
      }.getMessage
      assert(message.contains(expectedMessage))

      message = intercept[AnalysisException] {
        sql("ALTER TABLE transtbl SET SERDEPROPERTIES ('prop1'='1')")
      }.getMessage
      assert(message.contains(expectedMessage))

      message = intercept[AnalysisException] {
        sql("ALTER TABLE transtbl ADD PARTITION (code='x')")
      }.getMessage
      assert(message.contains(expectedMessage))

      message = intercept[AnalysisException] {
        sql("ALTER TABLE transtbl PARTITION (code='b') RENAME TO PARTITION (code='x')")
      }.getMessage
      assert(message.contains(expectedMessage))

      message = intercept[AnalysisException] {
        sql("ALTER TABLE transtbl DROP PARTITION (code='b')")
      }.getMessage
      assert(message.contains(expectedMessage))

      message = intercept[AnalysisException] {
        sql("ALTER TABLE transtbl RECOVER PARTITIONS")
      }.getMessage
      assert(message.contains(expectedMessage))

      message = intercept[AnalysisException] {
        sql("ANALYZE TABLE transtbl COMPUTE STATISTICS")
      }.getMessage
      assert(message.contains(expectedMessage))

      message = intercept[AnalysisException] {
        sql("ANALYZE TABLE transtbl COMPUTE STATISTICS FOR COLUMNS employeeID")
      }.getMessage
      assert(message.contains(expectedMessage))

      message = intercept[AnalysisException] {
        sql("ANALYZE TABLE transtbl PARTITION(code='b') COMPUTE STATISTICS")
      }.getMessage
      assert(message.contains(expectedMessage))

      message = intercept[AnalysisException] {
        df.write.insertInto("transtbl")
      }.getMessage
      assert(message.contains(expectedMessage))

      intercept[AnalysisException] {
        df.write.mode("append").saveAsTable("transtbl")
      }.getMessage
      assert(message.contains(expectedMessage))

      message = intercept[AnalysisException] {
        df.write.mode("overwrite").saveAsTable("transtbl")
      }.getMessage
      assert(message.contains(expectedMessage))
    }
  }

  private def testNoAccess(expectedMessage: String): Unit = {
    testRO(expectedMessage)

    // for the duration of these tests, turn ACID checking back on
    withSQLConf((SQLConf.CHECK_TRANSLATION_LAYER.key, "true")) {
      var message = intercept[AnalysisException] {
        sql("SELECT * FROM transtbl")
      }.getMessage
      assert(message.contains(expectedMessage))

      message = intercept[AnalysisException] {
        spark.read.table("transtbl").show
      }.getMessage
      assert(message.contains(expectedMessage))
    }
  }

  private def testWithDifferentReaders(
      configKey: Option[String],
      func: String => Unit,
      expectedMessage: String): Unit = {
    if (configKey.isEmpty) {
      func(expectedMessage)
    } else {
      // test the ACID table with both Native and Hive readers, since that
      // changes how the analyzer treats the logical plans
      Seq("true", "false").foreach { configValue =>
        withSQLConf((configKey.get, configValue)) {
          func(expectedMessage)
        }
      }
    }
  }

  test("CDPD-454: Respect access type from translation layer") {
    val tableTypes = Seq(("parquet", Some(HiveUtils.CONVERT_METASTORE_PARQUET.key)),
      ("orc", Some(HiveUtils.CONVERT_METASTORE_ORC.key)),
      ("textFile", None))

    tableTypes.foreach { case (format, configKey) =>
      // turn off translation layer check so that DROP TABLE, which is automatically called
      // by withTable(), doesn't fail. We will call a method that will
      // temporarily switch translation layer check back on for the actual tests.
      withSQLConf((SQLConf.CHECK_TRANSLATION_LAYER.key, "false")) {
        val expectedMessage = "no access"
        withTable("transtbl") {
          sql(
            s"""
               |CREATE TABLE transtbl (employeeID INT, employeeName STRING)
               |PARTITIONED BY (code STRING)
               |stored as $format
               |TBLPROPERTIES ('${CatalogUtils.TRANSLATION_LAYER_ACCESSTYPE_PROPERTY_TEST}'='1')
            """.stripMargin)
          testWithDifferentReaders(configKey, testNoAccess, expectedMessage)
        }
      }

      withSQLConf((SQLConf.CHECK_TRANSLATION_LAYER.key, "false")) {
        val expectedMessage = "only read access"
        withTable("transtbl") {
          sql(
            s"""
               |CREATE TABLE transtbl (employeeID INT, employeeName STRING)
               |PARTITIONED BY (code STRING)
               |stored as $format
               |TBLPROPERTIES ('${CatalogUtils.TRANSLATION_LAYER_ACCESSTYPE_PROPERTY_TEST}'='2')
            """.stripMargin)
          testWithDifferentReaders(configKey, testRO, expectedMessage)
        }
      }
    }
  }

  test("CDPD-454: Check that capabilities are set appropriately on Spark-created tables") {
    withTable("hiveincompatible", "hivecompatible") {
      Seq(1).toDF("a").write.format("json").saveAsTable("hiveincompatible")
      checkAnswer(
        sql(s"SHOW TBLPROPERTIES hiveincompatible('OBJCAPABILITIES')"),
        Row("SPARKSQL") :: Nil
      )
      Seq(1).toDF("a").write.format("orc").saveAsTable("hivecompatible")
      checkAnswer(
        sql(s"SHOW TBLPROPERTIES hivecompatible('OBJCAPABILITIES')"),
        Row("Table default.hivecompatible does not have property: OBJCAPABILITIES") :: Nil
      )
    }
  }

  test("CDPD-3196: Ensure virtual views can be dropped") {
    withTable("transtbl", "transview") {
      sql(
        s"""
           |CREATE TABLE transtbl (employeeID INT, employeeName STRING)
           |stored as orc
        """.stripMargin)
      sql(
        s"""
           |CREATE VIEW transview
           |TBLPROPERTIES ('${CatalogUtils.TRANSLATION_LAYER_ACCESSTYPE_PROPERTY_TEST}'='1')
           |AS SELECT * from transtbl
         """.stripMargin)
      withSQLConf((SQLConf.CHECK_TRANSLATION_LAYER.key, "true")) {
        sql("drop view transview")
      }
    }
  }
}

