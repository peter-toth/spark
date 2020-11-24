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

package org.apache.spark.sql.streaming.ui

import org.openqa.selenium.WebDriver
import org.openqa.selenium.htmlunit.HtmlUnitDriver
import org.scalatest._
import org.scalatest.concurrent.Eventually._
import org.scalatest.time.SpanSugar._
import org.scalatestplus.selenium.WebBrowser

import org.apache.spark._
import org.apache.spark.sql.LocalSparkSession.withSparkSession
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.util.quietly
import org.apache.spark.sql.internal.StaticSQLConf.ENABLED_STREAMING_UI_CUSTOM_METRIC_LIST
import org.apache.spark.sql.streaming.StreamingQueryException
import org.apache.spark.ui.SparkUICssErrorHandler

class UISeleniumSuite extends SparkFunSuite with WebBrowser with Matchers with BeforeAndAfterAll {

  implicit var webDriver: WebDriver = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    webDriver = new HtmlUnitDriver {
      getWebClient.setCssErrorHandler(new SparkUICssErrorHandler)
    }
  }

  private def newSparkSession(
      master: String = "local",
      additionalConfs: Map[String, String] = Map.empty): SparkSession = {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName("ui-test")
      .set("spark.ui.enabled", "true")
      .set("spark.ui.port", "0")
      .set(ENABLED_STREAMING_UI_CUSTOM_METRIC_LIST, Seq("stateOnCurrentVersionSizeBytes"))
    additionalConfs.foreach { case (k, v) => conf.set(k, v) }
    val spark = SparkSession.builder().master(master).config(conf).getOrCreate()
    assert(spark.sparkContext.ui.isDefined)
    spark
  }

  def goToUi(spark: SparkSession, path: String): Unit = {
    go to (spark.sparkContext.ui.get.webUrl.stripSuffix("/") + path)
  }

  test("SPARK-30984: Structured Streaming UI should be activated when running a streaming query") {
    quietly {
      withSparkSession(newSparkSession()) { spark =>
        import spark.implicits._
        try {
          spark.range(1, 10).count()

          goToUi(spark, "/StreamingQuery")

          val h3Text = findAll(cssSelector("h3")).map(_.text).toSeq
          h3Text should not contain ("Streaming Query")

          val input1 = spark.readStream.format("rate").load()
          val input2 = spark.readStream.format("rate").load()
          val activeQuery =
            input1.join(input2, "value").writeStream.format("console").start()
          val completedQuery =
            input1.join(input2, "value").writeStream.format("console").start()
          completedQuery.stop()
          val failedQuery = spark.readStream.format("rate").load().select("value").as[Long]
            .map(_ / 0).writeStream.format("console").start()
          try {
            failedQuery.awaitTermination()
          } catch {
            case _: StreamingQueryException =>
          }

          eventually(timeout(30.seconds), interval(100.milliseconds)) {
            // Check the query list page
            goToUi(spark, "/StreamingQuery")

            findAll(cssSelector("h3")).map(_.text).toSeq should contain("Streaming Query")
            findAll(cssSelector("""#activeQueries-table th""")).map(_.text).toSeq should be {
              List("Name", "Status", "Id", "Run ID", "Start Time", "Duration", "Avg Input /sec",
                "Avg Process /sec", "Lastest Batch")
            }
            val activeQueries =
              findAll(cssSelector("""#activeQueries-table td""")).map(_.text).toSeq
            activeQueries should contain(activeQuery.id.toString)
            activeQueries should contain(activeQuery.runId.toString)
            findAll(cssSelector("""#completedQueries-table th"""))
              .map(_.text).toSeq should be {
                List("Name", "Status", "Id", "Run ID", "Start Time", "Duration", "Avg Input /sec",
                  "Avg Process /sec", "Lastest Batch", "Error")
              }
            val completedQueries =
              findAll(cssSelector("""#completedQueries-table td""")).map(_.text).toSeq
            completedQueries should contain(completedQuery.id.toString)
            completedQueries should contain(completedQuery.runId.toString)
            completedQueries should contain(failedQuery.id.toString)
            completedQueries should contain(failedQuery.runId.toString)

            // Check the query statistics page
            val activeQueryLink =
              findAll(cssSelector("""#activeQueries-table a""")).flatMap(_.attribute("href")).next
            go to activeQueryLink

            findAll(cssSelector("h3"))
              .map(_.text).toSeq should contain("Streaming Query Statistics")
            val summaryText = findAll(cssSelector("div strong")).map(_.text).toSeq
            summaryText should contain ("Name:")
            summaryText should contain ("Id:")
            summaryText should contain ("RunId:")
            findAll(cssSelector("""#stat-table th""")).map(_.text).toSeq should be {
              List("", "Timelines", "Histograms")
            }
            summaryText should contain ("Input Rate (?)")
            summaryText should contain ("Process Rate (?)")
            summaryText should contain ("Input Rows (?)")
            summaryText should contain ("Batch Duration (?)")
            summaryText should contain ("Operation Duration (?)")
            summaryText should contain ("Aggregated Number Of Total State Rows (?)")
            summaryText should contain ("Aggregated Number Of Updated State Rows (?)")
            summaryText should contain ("Aggregated State Memory Used In Bytes (?)")
            summaryText should contain ("Aggregated Number Of Rows Dropped By Watermark (?)")
            summaryText should contain ("Aggregated Custom Metric stateOnCurrentVersionSizeBytes" +
              " (?)")
            summaryText should not contain ("Aggregated Custom Metric loadedMapCacheHitCount (?)")
            summaryText should not contain ("Aggregated Custom Metric loadedMapCacheMissCount (?)")
          }
        } finally {
          spark.streams.active.foreach(_.stop())
        }
      }
    }
  }

  override def afterAll(): Unit = {
    try {
      if (webDriver != null) {
        webDriver.quit()
      }
    } finally {
      super.afterAll()
    }
  }
}
