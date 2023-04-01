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

import java.io.{BufferedReader, File, FileInputStream, InputStreamReader}

import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hadoop.hive.metastore.InsertEventListener

import org.apache.spark.sql.{DataFrame, QueryTest, SaveMode}
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.util.Utils


class InsertEventGenTestSuite extends QueryTest with SQLTestUtils with
  TestHiveSingleton {

  private var scratchDir: String = _

  private var path: File = _

  import testImplicits._

  val previousConf = scala.collection.mutable.Map.empty[String, String]

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val key = "hive.exec.dynamic.partition.mode"
    previousConf += key -> spark.conf.get(key, "")
    spark.conf.set(key, "nonstrict")
    // scalastyle:off hadoopconfiguration
    scratchDir = spark.sparkContext.hadoopConfiguration.get(
      ConfVars.SCRATCHDIR.varname).substring("file:".length)
    // scalastyle:on hadoopconfiguration
    withTable(InsertEventListener.enableListener) {
      sql(s"CREATE TABLE ${InsertEventListener.enableListener} (key int, value string)" +
        s" using $format")
    }
  }

  override protected def afterAll(): Unit = {
    // disable InsertEventListener
    withTable(InsertEventListener.disableListener) {
      sql(s"CREATE TABLE ${InsertEventListener.disableListener} (key int, value string)" +
        s" using $format")
    }

    previousConf.foreach {
      case (key, value) => if (value.isEmpty) {
        spark.conf.unset(key)
      } else {
        spark.conf.set(key, value)
      }
    }
    super.afterAll()
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    path = Utils.createTempDir()
    path.delete()
  }

  protected def createTable(
      table: String,
      cols: Seq[String],
      colTypes: Seq[String],
      partCols: Seq[String] = Nil): Unit = {
    val values = cols.zip(colTypes).map(tuple => s"${tuple._1} ${tuple._2}").
      mkString("(", ", ", ")")
    val partitionSpec = if (partCols.nonEmpty) {
      partCols.mkString("PARTITIONED BY (", ",", ")")
    } else ""
    sql(s"CREATE TABLE $table$values USING $format $partitionSpec")
  }

  test("direct insert on partitioned table using sql statement") {
    withTable("t") {
      sql(s"CREATE TABLE t(i STRING, c string) USING $format PARTITIONED BY (c)")
      sql("INSERT INTO t PARTITION (c=null) VALUES ('1')")
      assertFromResultsFile(1)
    }
  }

  test("insert using dataframe api on non partitioned table") {
    withTable("createAndInsertTest") {
      sql(s"CREATE TABLE createAndInsertTest (key int, value string) using $format")
      // Add some data.
      testData.write.mode(SaveMode.Append).insertInto("createAndInsertTest")
      assertFromResultsFile(1)
    }
  }

  test("insert using dataframe api on partitioned table") {
    withTable("createAndInsertTest") {
      sql(s"CREATE TABLE createAndInsertTest (key int, value string) using" +
        s" $format partitioned by (key)")
      // TODO: Because of spark-43112, the column order is changed as a result the dataframe
      // used for insertion needs to have the column order swapped.
      // Once the bug is fixed, this test will have to be fixed too.
      // Add some data.
      testData.select($"value", $"key").write.mode(SaveMode.Append).insertInto(
        "createAndInsertTest")
      assertFromResultsFile(1)
    }
  }

  test("insert using dataframe api on non partitioned table using saveAs api") {
    withTable("createAndInsertTest") {
      testData.write.mode(SaveMode.Overwrite).format("hive").
        saveAsTable("createAndInsertTest")
      testData.write.mode(SaveMode.Append).format("hive").
        saveAsTable("createAndInsertTest")
      assertFromResultsFile(2)
    }
  }

  // TODO: stock spark generates n events for append. why?
  test("insert using dataframe api on partitioned table using saveAs api") {
    withTable("createAndInsertTest") {
      testData.write.mode(SaveMode.Overwrite).partitionBy("key").format("hive").
        saveAsTable("createAndInsertTest")
      assertFromResultsFile(1)
      testData.write.mode(SaveMode.Append).partitionBy("key").format("hive").
        saveAsTable("createAndInsertTest")
      // TODO: once the issue of per partition invocation is resolved correct the assertion to 2
      assertFromResultsFile(testData.count().toInt + 1)
    }
  }

  test("CREATE TABLE ... USING ... location (CTAS)") {
    withTable("t") {
      sql(
        s"""
           |CREATE  TABLE t USING hive
           |OPTIONS (fileFormat='parquet',PATH='${path.toURI}')
           |AS SELECT 1 AS a, 2 AS b
         """.stripMargin)
      assertFromResultsFile(1)
    }
  }

  test("CREATE partitioned TABLE ... USING ... location (CTAS)") {
    withTable("t") {
      sql(
        s"""
           |CREATE  TABLE t USING hive
           |OPTIONS (fileFormat='parquet',PATH='${path.toURI}')
           |PARTITIONED BY (a)
           |AS SELECT 1 AS a, 2 AS b
         """.stripMargin)
      assertFromResultsFile(1)
    }
  }

  def format: String = "hive OPTIONS(fileFormat='parquet')"

  private def assertFromResultsFile(expectedInsertEvents: Int): Unit = {
    val reader = new BufferedReader(new InputStreamReader(new FileInputStream(
      new File(scratchDir, InsertEventListener.resultFileName))))
    val actualCount = Integer.parseInt(reader.readLine().trim())
    assert(actualCount === expectedInsertEvents)
  }
}
