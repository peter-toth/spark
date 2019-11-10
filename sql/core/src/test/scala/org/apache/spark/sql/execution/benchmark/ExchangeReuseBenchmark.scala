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

package org.apache.spark.sql.execution.benchmark

import org.apache.spark.SparkConf
import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.SparkSession

object ExchangeReuseBenchmark extends SqlBasedBenchmark {

  override def getSparkSession: SparkSession = {
    val conf = new SparkConf()
      .setMaster("local[1]")
      .setAppName("test-sql-context")

    SparkSession.builder.config(conf).getOrCreate()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    import spark.implicits._

    val testDataSize = 5

    spark.sparkContext.parallelize(1 to testDataSize).map { i => Thread.sleep(1000); i }.toDF("key")
      .createOrReplaceTempView("slowView")

    val queryString =
      """
        SELECT
          (SELECT max(a.key) FROM slowView AS a JOIN slowView AS b ON b.key = a.key),
          a.key
        FROM slowView AS a
        JOIN slowView AS b ON b.key = a.key
      """.stripMargin

    val benchmark = new Benchmark(s"Exchange reuse 1", testDataSize)
    Seq(true, false).foreach { originalVersion =>
      benchmark.addCase(s"originalReuseExchangeVersion - $originalVersion") { _ =>
        withSQLConf("originalReuseExchangeVersion" -> originalVersion.toString) {
          spark.sql(queryString).collect()
        }
      }
    }
    benchmark.run()

    val queryString2 =
      """
        SELECT
          (SELECT min(a.key) FROM slowView AS a JOIN slowView AS b ON b.key = a.key),
          (SELECT max(a.key) FROM slowView AS a JOIN slowView AS b ON b.key = a.key)
      """.stripMargin

    val benchmark2 = new Benchmark(s"Exchange reuse 2", testDataSize)
    Seq(true, false).foreach { originalVersion =>
      benchmark2.addCase(s"originalReuseExchangeVersion - $originalVersion") { _ =>
        withSQLConf("originalReuseExchangeVersion" -> originalVersion.toString) {
          spark.sql(queryString2).collect()
        }
      }
    }
    benchmark2.run()
  }
}
