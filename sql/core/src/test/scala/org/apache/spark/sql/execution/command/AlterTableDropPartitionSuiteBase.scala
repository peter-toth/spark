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

package org.apache.spark.sql.execution.command

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.catalyst.analysis.NoSuchPartitionsException
import org.apache.spark.sql.internal.SQLConf

/**
 * This base suite contains unified tests for the `ALTER TABLE .. DROP PARTITION` command that
 * check V1 and V2 table catalogs. The tests that cannot run for all supported catalogs are
 * located in more specific test suites:
 *
 *   - V2 table catalog tests:
 *     `org.apache.spark.sql.execution.command.v2.AlterTableDropPartitionSuite`
 *   - V1 table catalog tests:
 *     `org.apache.spark.sql.execution.command.v1.AlterTableDropPartitionSuiteBase`
 *     - V1 In-Memory catalog:
 *       `org.apache.spark.sql.execution.command.v1.AlterTableDropPartitionSuite`
 *     - V1 Hive External catalog:
 *       `org.apache.spark.sql.hive.execution.command.AlterTableDropPartitionSuite`
 */
trait AlterTableDropPartitionSuiteBase extends QueryTest with DDLCommandTestUtils {
  override val command = "ALTER TABLE .. DROP PARTITION"

  protected def notFullPartitionSpecErr: String
  protected def nullPartitionValue: String

  protected def checkDropPartition(
      t: String,
      ifExists: String,
      specs: Map[String, Any]*): Unit = {
    checkPartitions(t, specs.map(_.mapValues(_.toString).toMap): _*)
    val specStr = specs.map(
      _.map {
        case (k, v: String) => s"$k = '$v'"
        case (k, v) => s"$k = $v"
      }.mkString("PARTITION (", ", ", ")"))
      .mkString(", ")
    sql(s"ALTER TABLE $t DROP $ifExists $specStr")
    checkPartitions(t)
  }

  test("single partition") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (id bigint, data string) $defaultUsing PARTITIONED BY (id)")
      Seq("", "IF EXISTS").foreach { ifExists =>
        sql(s"ALTER TABLE $t ADD PARTITION (id=1) LOCATION 'loc'")
        checkDropPartition(t, ifExists, Map("id" -> 1))
      }
    }
  }

  test("multiple partitions") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (id bigint, data string) $defaultUsing PARTITIONED BY (id)")
      Seq("", "IF EXISTS").foreach { ifExists =>
        sql(s"""
          |ALTER TABLE $t ADD
          |PARTITION (id=1) LOCATION 'loc'
          |PARTITION (id=2) LOCATION 'loc1'""".stripMargin)
        checkDropPartition(t, ifExists, Map("id" -> 1), Map("id" -> 2))
      }
    }
  }

  test("multi-part partition") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (id bigint, a int, b string) $defaultUsing PARTITIONED BY (a, b)")
      Seq("", "IF EXISTS").foreach { ifExists =>
        sql(s"ALTER TABLE $t ADD PARTITION (a = 2, b = 'abc')")
        checkDropPartition(t, ifExists, Map("a" -> 2, "b" -> "abc"))
      }
    }
  }

  test("table to alter does not exist") {
    withNamespaceAndTable("ns", "does_not_exist") { t =>
      val errMsg = intercept[AnalysisException] {
        sql(s"ALTER TABLE $t DROP PARTITION (a='4', b='9')")
      }.getMessage
      assert(errMsg.contains("Table not found"))
    }
  }

  test("case sensitivity in resolving partition specs") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (id bigint, data string) $defaultUsing PARTITIONED BY (id)")
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
        val errMsg = intercept[AnalysisException] {
          sql(s"ALTER TABLE $t DROP PARTITION (ID=1)")
        }.getMessage
        assert(errMsg.contains("ID is not a valid partition column"))
      }

      withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
        Seq("", "IF EXISTS").foreach { ifExists =>
          sql(s"ALTER TABLE $t ADD PARTITION (ID=1) LOCATION 'loc1'")
          checkDropPartition(t, ifExists, Map("id" -> 1))
        }
      }
    }
  }

  test("SPARK-33676: not fully specified partition spec") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"""
        |CREATE TABLE $t (id bigint, part0 int, part1 string)
        |$defaultUsing
        |PARTITIONED BY (part0, part1)""".stripMargin)
      val errMsg = intercept[AnalysisException] {
        sql(s"ALTER TABLE $t DROP PARTITION (part0 = 1)")
      }.getMessage
      assert(errMsg.contains(notFullPartitionSpecErr))
    }
  }

  test("partition not exists") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (id bigint, data string) $defaultUsing PARTITIONED BY (id)")
      sql(s"ALTER TABLE $t ADD PARTITION (id=1) LOCATION 'loc'")

      val errMsg = intercept[NoSuchPartitionsException] {
        sql(s"ALTER TABLE $t DROP PARTITION (id=1), PARTITION (id=2)")
      }.getMessage
      assert(errMsg.contains("partitions not found in table"))

      checkPartitions(t, Map("id" -> "1"))
      sql(s"ALTER TABLE $t DROP IF EXISTS PARTITION (id=1), PARTITION (id=2)")
      checkPartitions(t)
    }
  }

  test("SPARK-33990: don not return data from dropped partition") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (id int, part int) $defaultUsing PARTITIONED BY (part)")
      sql(s"INSERT INTO $t PARTITION (part=0) SELECT 0")
      sql(s"INSERT INTO $t PARTITION (part=1) SELECT 1")
      QueryTest.checkAnswer(sql(s"SELECT * FROM $t"), Seq(Row(0, 0), Row(1, 1)))
      sql(s"ALTER TABLE $t DROP PARTITION (part=0)")
      QueryTest.checkAnswer(sql(s"SELECT * FROM $t"), Seq(Row(1, 1)))
    }
  }

  test("SPARK-33950, SPARK-33987: refresh cache after partition dropping") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (id int, part int) $defaultUsing PARTITIONED BY (part)")
      sql(s"INSERT INTO $t PARTITION (part=0) SELECT 0")
      sql(s"INSERT INTO $t PARTITION (part=1) SELECT 1")
      assert(!spark.catalog.isCached(t))
      sql(s"CACHE TABLE $t")
      assert(spark.catalog.isCached(t))
      QueryTest.checkAnswer(sql(s"SELECT * FROM $t"), Seq(Row(0, 0), Row(1, 1)))
      sql(s"ALTER TABLE $t DROP PARTITION (part=0)")
      assert(spark.catalog.isCached(t))
      QueryTest.checkAnswer(sql(s"SELECT * FROM $t"), Seq(Row(1, 1)))
    }
  }

  test("SPARK-33591: null as a partition value") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (col1 INT, p1 STRING) $defaultUsing PARTITIONED BY (p1)")
      sql(s"ALTER TABLE $t ADD PARTITION (p1 = null)")
      checkPartitions(t, Map("p1" -> nullPartitionValue))
      sql(s"ALTER TABLE $t DROP PARTITION (p1 = null)")
      checkPartitions(t)
    }
  }
}
