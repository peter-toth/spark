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

package org.apache.spark.sql.execution

import java.io.{BufferedWriter, OutputStreamWriter}
import java.nio.charset.StandardCharsets
import java.sql.{Date, Timestamp}
import java.util.concurrent.atomic.AtomicLong

import org.apache.hadoop.fs.Path

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.UnsupportedOperationChecker
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, ReturnAnswer}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.catalyst.util.StringUtils.{PlanStringConcat, StringConcat}
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.execution.command.{DescribeTableCommand, ExecutedCommandExec, ShowTablesCommand}
import org.apache.spark.sql.execution.dynamicpruning.PlanDynamicPruningFilters
import org.apache.spark.sql.execution.exchange.{EnsureRequirements, ReuseExchange}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{BinaryType, DateType, DecimalType, TimestampType, _}
import org.apache.spark.util.Utils

/**
 * The primary workflow for executing relational queries using Spark.  Designed to allow easy
 * access to the intermediate phases of query execution for developers.
 *
 * While this is not a public class, we should avoid changing the function names for the sake of
 * changing them, because a lot of developers use the feature for debugging.
 */
class QueryExecution(val sparkSession: SparkSession, val logical: LogicalPlan) {

  val id: Long = QueryExecution.nextExecutionId

  // TODO: Move the planner an optimizer into here from SessionState.
  protected def planner = sparkSession.sessionState.planner

  def assertAnalyzed(): Unit = analyzed

  def assertSupported(): Unit = {
    if (sparkSession.sessionState.conf.isUnsupportedOperationCheckEnabled) {
      UnsupportedOperationChecker.checkForBatch(analyzed)
    }
  }

  lazy val analyzed: LogicalPlan = {
    SparkSession.setActiveSession(sparkSession)
    sparkSession.sessionState.analyzer.executeAndCheck(logical)
  }

  lazy val withCachedData: LogicalPlan = {
    assertAnalyzed()
    assertSupported()
    sparkSession.sharedState.cacheManager.useCachedData(analyzed)
  }

  lazy val optimizedPlan: LogicalPlan = sparkSession.sessionState.optimizer.execute(withCachedData)

  lazy val sparkPlan: SparkPlan = {
    SparkSession.setActiveSession(sparkSession)
    // TODO: We use next(), i.e. take the first plan returned by the planner, here for now,
    //       but we will implement to choose the best plan.
    planner.plan(ReturnAnswer(optimizedPlan)).next()
  }

  // executedPlan should not be used to initialize any SparkPlan. It should be
  // only used for execution.
  lazy val executedPlan: SparkPlan = prepareForExecution(sparkPlan)

  /** Internal version of the RDD. Avoids copies and has no schema */
  lazy val toRdd: RDD[InternalRow] = {
    if (sparkSession.sessionState.conf.getConf(SQLConf.USE_CONF_ON_RDD_OPERATION)) {
      new SQLExecutionRDD(executedPlan.execute(), sparkSession.sessionState.conf)
    } else {
      executedPlan.execute()
    }
  }

  /**
   * Prepares a planned [[SparkPlan]] for execution by inserting shuffle operations and internal
   * row format conversions as needed.
   */
  protected def prepareForExecution(plan: SparkPlan): SparkPlan = {
    preparations.foldLeft(plan) { case (sp, rule) => rule.apply(sp) }
  }

  /** A sequence of rules that will be applied in order to the physical plan before execution. */
  protected def preparations: Seq[Rule[SparkPlan]] = Seq(
    PlanDynamicPruningFilters(sparkSession),
    PlanSubqueries(sparkSession),
    EnsureRequirements(sparkSession.sessionState.conf),
    CollapseCodegenStages(sparkSession.sessionState.conf),
    ReuseExchange(sparkSession.sessionState.conf),
    ReuseSubquery(sparkSession.sessionState.conf))

  /**
   * Returns the result as a hive compatible sequence of strings. This is used in tests and
   * `SparkSQLDriver` for CLI applications.
   */
  def hiveResultString(): Seq[String] = executedPlan match {
    case ExecutedCommandExec(desc: DescribeTableCommand) =>
      // If it is a describe command for a Hive table, we want to have the output format
      // be similar with Hive.
      desc.run(sparkSession).map {
        case Row(name: String, dataType: String, comment) =>
          Seq(name, dataType,
            Option(comment.asInstanceOf[String]).getOrElse(""))
            .map(s => String.format(s"%-20s", s))
            .mkString("\t")
      }
    // SHOW TABLES in Hive only output table names, while ours output database, table name, isTemp.
    case command @ ExecutedCommandExec(s: ShowTablesCommand) if !s.isExtended =>
      command.executeCollect().map(_.getString(1))
    case other =>
      val result: Seq[Seq[Any]] = other.executeCollectPublic().map(_.toSeq).toSeq
      // We need the types so we can output struct field names
      val types = analyzed.output.map(_.dataType)
      // Reformat to match hive tab delimited output.
      result.map(_.zip(types).map(toHiveString)).map(_.mkString("\t"))
  }

  /** Formats a datum (based on the given data type) and returns the string representation. */
  private def toHiveString(a: (Any, DataType)): String = {
    val primitiveTypes = Seq(StringType, IntegerType, LongType, DoubleType, FloatType,
      BooleanType, ByteType, ShortType, DateType, TimestampType, BinaryType)

    def formatDecimal(d: java.math.BigDecimal): String = {
      if (d.compareTo(java.math.BigDecimal.ZERO) == 0) {
        java.math.BigDecimal.ZERO.toPlainString
      } else {
        d.stripTrailingZeros().toPlainString
      }
    }

    /** Hive outputs fields of structs slightly differently than top level attributes. */
    def toHiveStructString(a: (Any, DataType)): String = a match {
      case (struct: Row, StructType(fields)) =>
        struct.toSeq.zip(fields).map {
          case (v, t) => s""""${t.name}":${toHiveStructString((v, t.dataType))}"""
        }.mkString("{", ",", "}")
      case (seq: Seq[_], ArrayType(typ, _)) =>
        seq.map(v => (v, typ)).map(toHiveStructString).mkString("[", ",", "]")
      case (map: Map[_, _], MapType(kType, vType, _)) =>
        map.map {
          case (key, value) =>
            toHiveStructString((key, kType)) + ":" + toHiveStructString((value, vType))
        }.toSeq.sorted.mkString("{", ",", "}")
      case (null, _) => "null"
      case (s: String, StringType) => "\"" + s + "\""
      case (decimal, DecimalType()) => decimal.toString
      case (interval, CalendarIntervalType) => interval.toString
      case (other, tpe) if primitiveTypes contains tpe => other.toString
    }

    a match {
      case (struct: Row, StructType(fields)) =>
        struct.toSeq.zip(fields).map {
          case (v, t) => s""""${t.name}":${toHiveStructString((v, t.dataType))}"""
        }.mkString("{", ",", "}")
      case (seq: Seq[_], ArrayType(typ, _)) =>
        seq.map(v => (v, typ)).map(toHiveStructString).mkString("[", ",", "]")
      case (map: Map[_, _], MapType(kType, vType, _)) =>
        map.map {
          case (key, value) =>
            toHiveStructString((key, kType)) + ":" + toHiveStructString((value, vType))
        }.toSeq.sorted.mkString("{", ",", "}")
      case (null, _) => "NULL"
      case (d: Date, DateType) =>
        DateTimeUtils.dateToString(DateTimeUtils.fromJavaDate(d))
      case (t: Timestamp, TimestampType) =>
        DateTimeUtils.timestampToString(DateTimeUtils.fromJavaTimestamp(t),
          DateTimeUtils.getTimeZone(sparkSession.sessionState.conf.sessionLocalTimeZone))
      case (bin: Array[Byte], BinaryType) => new String(bin, StandardCharsets.UTF_8)
      case (decimal: java.math.BigDecimal, DecimalType()) => formatDecimal(decimal)
      case (interval, CalendarIntervalType) => interval.toString
      case (other, tpe) if primitiveTypes.contains(tpe) => other.toString
    }
  }

  def simpleString: String = withRedaction {
    val concat = new PlanStringConcat()
    concat.append("== Physical Plan ==\n")
    QueryPlan.append(executedPlan, concat.append, verbose = false, addSuffix = false)
    concat.append("\n")
    concat.toString
  }

  private def writePlans(append: String => Unit, maxFields: Int): Unit = {
    val (verbose, addSuffix) = (true, false)
    append("== Parsed Logical Plan ==\n")
    QueryPlan.append(logical, append, verbose, addSuffix, maxFields)
    append("\n== Analyzed Logical Plan ==\n")
    val analyzedOutput = try {
      truncatedString(
        analyzed.output.map(o => s"${o.name}: ${o.dataType.simpleString}"), ", ", maxFields)
    } catch {
      case e: AnalysisException => e.toString
    }
    append(analyzedOutput)
    append("\n")
    QueryPlan.append(analyzed, append, verbose, addSuffix, maxFields)
    append("\n== Optimized Logical Plan ==\n")
    QueryPlan.append(optimizedPlan, append, verbose, addSuffix, maxFields)
    append("\n== Physical Plan ==\n")
    QueryPlan.append(executedPlan, append, verbose, addSuffix, maxFields)
  }

  override def toString: String = withRedaction {
    val concat = new PlanStringConcat()
    writePlans(concat.append, SQLConf.get.maxToStringFields)
    concat.toString
  }

  def stringWithStats: String = withRedaction {
    val concat = new PlanStringConcat()
    val maxFields = SQLConf.get.maxToStringFields

    // trigger to compute stats for logical plans
    optimizedPlan.stats

    // only show optimized logical plan and physical plan
    concat.append("== Optimized Logical Plan ==\n")
    QueryPlan.append(optimizedPlan, concat.append, verbose = true, addSuffix = true, maxFields)
    concat.append("\n== Physical Plan ==\n")
    QueryPlan.append(executedPlan, concat.append, verbose = true, addSuffix = false, maxFields)
    concat.append("\n")
    concat.toString
  }

  /**
   * Redact the sensitive information in the given string.
   */
  private def withRedaction(message: String): String = {
    Utils.redact(sparkSession.sessionState.conf.stringRedactionPattern, message)
  }

  /** A special namespace for commands that can be used to debug query execution. */
  // scalastyle:off
  object debug {
    // scalastyle:on

    /**
     * Prints to stdout all the generated code found in this plan (i.e. the output of each
     * WholeStageCodegen subtree).
     */
    def codegen(): Unit = {
      // scalastyle:off println
      println(org.apache.spark.sql.execution.debug.codegenString(executedPlan))
      // scalastyle:on println
    }

    /**
     * Get WholeStageCodegenExec subtrees and the codegen in a query plan
     *
     * @return Sequence of WholeStageCodegen subtrees and corresponding codegen
     */
    def codegenToSeq(): Seq[(String, String)] = {
      org.apache.spark.sql.execution.debug.codegenStringSeq(executedPlan)
    }

    /**
     * Dumps debug information about query execution into the specified file.
     *
     * @param maxFields maximum number of fields converted to string representation.
     */
    def toFile(path: String, maxFields: Int = Int.MaxValue): Unit = {
      val filePath = new Path(path)
      val fs = filePath.getFileSystem(sparkSession.sessionState.newHadoopConf())
      val writer = new BufferedWriter(new OutputStreamWriter(fs.create(filePath)))
      val append = (s: String) => {
        writer.write(s)
      }
      try {
        writePlans(append, maxFields)
        writer.write("\n== Whole Stage Codegen ==\n")
        org.apache.spark.sql.execution.debug.writeCodegen(writer.write, executedPlan)
      } finally {
        writer.close()
      }
    }
  }
}

object QueryExecution {
  private val _nextExecutionId = new AtomicLong(0)

  private def nextExecutionId: Long = _nextExecutionId.getAndIncrement
}
