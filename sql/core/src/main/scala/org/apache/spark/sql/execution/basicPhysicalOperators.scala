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

import java.util.concurrent.TimeUnit._

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.Duration

import org.apache.spark._
import org.apache.spark.rdd.{EmptyRDD, PartitionwiseSampledRDD, RDD}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.BindReferences.bindReferences
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.{Except, LogicalPlan, RecursiveReference, Statistics}
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.EstimationUtils
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{LongType, StructType}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.ThreadUtils
import org.apache.spark.util.random.{BernoulliCellSampler, PoissonSampler}

/** Physical plan for Project. */
case class ProjectExec(projectList: Seq[NamedExpression], child: SparkPlan)
  extends UnaryExecNode with CodegenSupport {

  override def output: Seq[Attribute] = projectList.map(_.toAttribute)

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    child.asInstanceOf[CodegenSupport].inputRDDs()
  }

  protected override def doProduce(ctx: CodegenContext): String = {
    child.asInstanceOf[CodegenSupport].produce(ctx, this)
  }

  override def usedInputs: AttributeSet = {
    // only the attributes those are used at least twice should be evaluated before this plan,
    // otherwise we could defer the evaluation until output attribute is actually used.
    val usedExprIds = projectList.flatMap(_.collect {
      case a: Attribute => a.exprId
    })
    val usedMoreThanOnce = usedExprIds.groupBy(id => id).filter(_._2.size > 1).keySet
    references.filter(a => usedMoreThanOnce.contains(a.exprId))
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    val exprs = bindReferences[Expression](projectList, child.output)
    val resultVars = exprs.map(_.genCode(ctx))
    // Evaluation of non-deterministic expressions can't be deferred.
    val nonDeterministicAttrs = projectList.filterNot(_.deterministic).map(_.toAttribute)
    s"""
       |${evaluateRequiredVariables(output, resultVars, AttributeSet(nonDeterministicAttrs))}
       |${consume(ctx, resultVars)}
     """.stripMargin
  }

  protected override def doExecute(): RDD[InternalRow] = {
    child.execute().mapPartitionsWithIndexInternal { (index, iter) =>
      val project = UnsafeProjection.create(projectList, child.output)
      project.initialize(index)
      iter.map(project)
    }
  }

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def outputPartitioning: Partitioning = child.outputPartitioning
}


/** Physical plan for Filter. */
case class FilterExec(condition: Expression, child: SparkPlan)
  extends UnaryExecNode with CodegenSupport with PredicateHelper {

  // Split out all the IsNotNulls from condition.
  private val (notNullPreds, otherPreds) = splitConjunctivePredicates(condition).partition {
    case IsNotNull(a) => isNullIntolerant(a) && a.references.subsetOf(child.outputSet)
    case _ => false
  }

  // If one expression and its children are null intolerant, it is null intolerant.
  private def isNullIntolerant(expr: Expression): Boolean = expr match {
    case e: NullIntolerant => e.children.forall(isNullIntolerant)
    case _ => false
  }

  // The columns that will filtered out by `IsNotNull` could be considered as not nullable.
  private val notNullAttributes = notNullPreds.flatMap(_.references).distinct.map(_.exprId)

  // Mark this as empty. We'll evaluate the input during doConsume(). We don't want to evaluate
  // all the variables at the beginning to take advantage of short circuiting.
  override def usedInputs: AttributeSet = AttributeSet.empty

  override def output: Seq[Attribute] = {
    child.output.map { a =>
      if (a.nullable && notNullAttributes.contains(a.exprId)) {
        a.withNullability(false)
      } else {
        a
      }
    }
  }

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    child.asInstanceOf[CodegenSupport].inputRDDs()
  }

  protected override def doProduce(ctx: CodegenContext): String = {
    child.asInstanceOf[CodegenSupport].produce(ctx, this)
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    val numOutput = metricTerm(ctx, "numOutputRows")

    /**
     * Generates code for `c`, using `in` for input attributes and `attrs` for nullability.
     */
    def genPredicate(c: Expression, in: Seq[ExprCode], attrs: Seq[Attribute]): String = {
      val bound = BindReferences.bindReference(c, attrs)
      val evaluated = evaluateRequiredVariables(child.output, in, c.references)

      // Generate the code for the predicate.
      val ev = ExpressionCanonicalizer.execute(bound).genCode(ctx)
      val nullCheck = if (bound.nullable) {
        s"${ev.isNull} || "
      } else {
        s""
      }

      s"""
         |$evaluated
         |${ev.code}
         |if (${nullCheck}!${ev.value}) continue;
       """.stripMargin
    }

    // To generate the predicates we will follow this algorithm.
    // For each predicate that is not IsNotNull, we will generate them one by one loading attributes
    // as necessary. For each of both attributes, if there is an IsNotNull predicate we will
    // generate that check *before* the predicate. After all of these predicates, we will generate
    // the remaining IsNotNull checks that were not part of other predicates.
    // This has the property of not doing redundant IsNotNull checks and taking better advantage of
    // short-circuiting, not loading attributes until they are needed.
    // This is very perf sensitive.
    // TODO: revisit this. We can consider reordering predicates as well.
    val generatedIsNotNullChecks = new Array[Boolean](notNullPreds.length)
    val generated = otherPreds.map { c =>
      val nullChecks = c.references.map { r =>
        val idx = notNullPreds.indexWhere { n => n.asInstanceOf[IsNotNull].child.semanticEquals(r)}
        if (idx != -1 && !generatedIsNotNullChecks(idx)) {
          generatedIsNotNullChecks(idx) = true
          // Use the child's output. The nullability is what the child produced.
          genPredicate(notNullPreds(idx), input, child.output)
        } else {
          ""
        }
      }.mkString("\n").trim

      // Here we use *this* operator's output with this output's nullability since we already
      // enforced them with the IsNotNull checks above.
      s"""
         |$nullChecks
         |${genPredicate(c, input, output)}
       """.stripMargin.trim
    }.mkString("\n")

    val nullChecks = notNullPreds.zipWithIndex.map { case (c, idx) =>
      if (!generatedIsNotNullChecks(idx)) {
        genPredicate(c, input, child.output)
      } else {
        ""
      }
    }.mkString("\n")

    // Reset the isNull to false for the not-null columns, then the followed operators could
    // generate better code (remove dead branches).
    val resultVars = input.zipWithIndex.map { case (ev, i) =>
      if (notNullAttributes.contains(child.output(i).exprId)) {
        ev.isNull = FalseLiteral
      }
      ev
    }

    // Note: wrap in "do { } while(false);", so the generated checks can jump out with "continue;"
    s"""
       |do {
       |  $generated
       |  $nullChecks
       |  $numOutput.add(1);
       |  ${consume(ctx, resultVars)}
       |} while(false);
     """.stripMargin
  }

  protected override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    child.execute().mapPartitionsWithIndexInternal { (index, iter) =>
      val predicate = newPredicate(condition, child.output)
      predicate.initialize(0)
      iter.filter { row =>
        val r = predicate.eval(row)
        if (r) numOutputRows += 1
        r
      }
    }
  }

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def outputPartitioning: Partitioning = child.outputPartitioning
}

/**
 * Physical plan node for a recursive table that encapsulates the physical plans of the anchor
 * terms and the logical plans of the recursive terms and the maximum number of rows to return.
 *
 * Anchor terms are physical plans and they are used to initialize the query in the first run.
 * Recursive terms are used to extend the result with new rows, They are logical plans and contain
 * references to the result of the previous iteration or to the so far cumulated result. These
 * references are updated with new statistics and compiled to physical plan and then updated to
 * reflect the appropriate RDD before execution.
 *
 * The execution terminates once the anchor terms or the current iteration of the recursive terms
 * return no rows or the number of cumulated rows reaches the limit.
 *
 * During the execution of a recursive query the previously computed results are reused multiple
 * times. To avoid massive recomputation of these pieces of the final result, they are cached.
 *
 * @param name the name of the recursive table
 * @param anchorTerms this child is used for initializing the query
 * @param recursiveTerms this child is used for extending the set of results with new rows based on
 *                       the results of the previous iteration (or the anchor in the first
 *                       iteration)
 * @param limit the maximum number of rows to return
 */
case class RecursiveTableExec(
    name: String,
    anchorTerms: Seq[SparkPlan],
    @transient
    val recursiveTerms: Seq[LogicalPlan],
    limit: Option[Long]) extends SparkPlan {
  override def children: Seq[SparkPlan] = anchorTerms

  override def output: Seq[Attribute] = anchorTerms.head.output.map(_.withNullability(true))

  override def simpleString(maxFields: Int): String =
    s"RecursiveTable $name${limit.map(", " + _).getOrElse("")}"

  override def innerChildren: Seq[QueryPlan[_]] = recursiveTerms ++ super.innerChildren

  override protected def doExecute(): RDD[InternalRow] = {
    val storageLevel = StorageLevel.fromString(conf.getConf(SQLConf.RECURSION_CACHE_STORAGE_LEVEL))

    val prevIterationRDDs = ArrayBuffer.empty[RDD[InternalRow]]
    var prevIterationCount = 0L

    val anchorTermsIterator = anchorTerms.iterator
    while (anchorTermsIterator.hasNext && limit.forall(_ > prevIterationCount)) {
      val anchorTerm = anchorTermsIterator.next()

      lazy val cumulatedResult = if (prevIterationRDDs.size > 1) {
        sparkContext.union(prevIterationRDDs)
      } else {
        prevIterationRDDs.head
      }

      anchorTerm.foreach {
        case rr: RecursiveReferenceExec if rr.name == name => rr.recursiveTable = cumulatedResult
        case _ =>
      }

      val rdd = anchorTerm.execute().map(_.copy()).persist(storageLevel)
      val count = rdd.count()
      if (count > 0) {
        prevIterationRDDs += rdd
        prevIterationCount += count
      }
    }

    val cumulatedRDDs = ArrayBuffer(prevIterationRDDs: _*)
    var cumulatedCount = prevIterationCount
    var level = 0
    val levelLimit = conf.getConf(SQLConf.RECURSION_LEVEL_LIMIT)
    while (prevIterationCount > 0 && limit.forall(_ > cumulatedCount)) {
      if (levelLimit != -1 && level > levelLimit) {
        throw new SparkException(s"Recursion level limit ${levelLimit} reached but query has not " +
          s"exhausted, try increasing ${SQLConf.RECURSION_LEVEL_LIMIT.key}")
      }

      val prevIterationResult = if (prevIterationRDDs.size > 1) {
        sparkContext.union(prevIterationRDDs)
      } else {
        prevIterationRDDs.head
      }

      prevIterationRDDs.clear()
      prevIterationCount = 0
      val recursiveTermsIterator = recursiveTerms.iterator
      while (recursiveTermsIterator.hasNext
        && limit.forall(_ > cumulatedCount + prevIterationCount)) {
        val recursiveTerm = recursiveTermsIterator.next()

        recursiveTerm.foreach {
          case rr: RecursiveReference if rr.name == name && !rr.cumulated =>
            rr.statistics = Statistics(
              EstimationUtils.getSizePerRow(output) * prevIterationCount,
              Some(prevIterationCount)
            )
          case rr: RecursiveReference if rr.name == name =>
            rr.statistics = Statistics(
              EstimationUtils.getSizePerRow(output) * (cumulatedCount + prevIterationCount),
              Some(cumulatedCount + prevIterationCount)
            )
          case _ =>
        }

        val physicalRecursiveTerm =
          new QueryExecution(sqlContext.sparkSession, recursiveTerm, alreadyOptimized = true)
            .executedPlan

        lazy val cumulatedResult = if (cumulatedRDDs.size + prevIterationRDDs.size > 1) {
          sparkContext.union(cumulatedRDDs ++ prevIterationRDDs)
        } else {
          cumulatedRDDs.head
        }

        physicalRecursiveTerm.foreach {
          case rr: RecursiveReferenceExec if rr.name == name && !rr.cumulated =>
            rr.recursiveTable = prevIterationResult
          case rr: RecursiveReferenceExec if rr.name == name => rr.recursiveTable = cumulatedResult
          case _ =>
        }

        val rdd = physicalRecursiveTerm.execute().map(_.copy()).persist(storageLevel)
        val count = rdd.count()
        if (count > 0) {
          prevIterationRDDs += rdd
          prevIterationCount += count
        }
      }

      cumulatedRDDs ++= prevIterationRDDs
      cumulatedCount += prevIterationCount
      level = level + 1
    }

    sparkContext.union(cumulatedRDDs)
  }
}

/** Physical plan for RecursiveReference. */
case class RecursiveReferenceExec(
    name: String,
    output: Seq[Attribute],
    cumulated: Boolean) extends LeafExecNode {
  // this will be updated in RecursiveTableExec before the actual execution
  @transient
  var recursiveTable: RDD[InternalRow] = _

  override protected def doExecute(): RDD[InternalRow] = recursiveTable
}

/**
 * Physical plan for sampling the dataset.
 *
 * @param lowerBound Lower-bound of the sampling probability (usually 0.0)
 * @param upperBound Upper-bound of the sampling probability. The expected fraction sampled
 *                   will be ub - lb.
 * @param withReplacement Whether to sample with replacement.
 * @param seed the random seed
 * @param child the SparkPlan
 */
case class SampleExec(
    lowerBound: Double,
    upperBound: Double,
    withReplacement: Boolean,
    seed: Long,
    child: SparkPlan) extends UnaryExecNode with CodegenSupport {
  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  protected override def doExecute(): RDD[InternalRow] = {
    if (withReplacement) {
      // Disable gap sampling since the gap sampling method buffers two rows internally,
      // requiring us to copy the row, which is more expensive than the random number generator.
      new PartitionwiseSampledRDD[InternalRow, InternalRow](
        child.execute(),
        new PoissonSampler[InternalRow](upperBound - lowerBound, useGapSamplingIfPossible = false),
        preservesPartitioning = true,
        seed)
    } else {
      child.execute().randomSampleWithRange(lowerBound, upperBound, seed)
    }
  }

  // Mark this as empty. This plan doesn't need to evaluate any inputs and can defer the evaluation
  // to the parent operator.
  override def usedInputs: AttributeSet = AttributeSet.empty

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    child.asInstanceOf[CodegenSupport].inputRDDs()
  }

  protected override def doProduce(ctx: CodegenContext): String = {
    child.asInstanceOf[CodegenSupport].produce(ctx, this)
  }

  override def needCopyResult: Boolean = withReplacement

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    val numOutput = metricTerm(ctx, "numOutputRows")

    if (withReplacement) {
      val samplerClass = classOf[PoissonSampler[UnsafeRow]].getName
      val initSampler = ctx.freshName("initSampler")

      // Inline mutable state since not many Sample operations in a task
      val sampler = ctx.addMutableState(s"$samplerClass<UnsafeRow>", "sampleReplace",
        v => {
          val initSamplerFuncName = ctx.addNewFunction(initSampler,
            s"""
              | private void $initSampler() {
              |   $v = new $samplerClass<UnsafeRow>($upperBound - $lowerBound, false);
              |   java.util.Random random = new java.util.Random(${seed}L);
              |   long randomSeed = random.nextLong();
              |   int loopCount = 0;
              |   while (loopCount < partitionIndex) {
              |     randomSeed = random.nextLong();
              |     loopCount += 1;
              |   }
              |   $v.setSeed(randomSeed);
              | }
           """.stripMargin.trim)
          s"$initSamplerFuncName();"
        }, forceInline = true)

      val samplingCount = ctx.freshName("samplingCount")
      s"""
         | int $samplingCount = $sampler.sample();
         | while ($samplingCount-- > 0) {
         |   $numOutput.add(1);
         |   ${consume(ctx, input)}
         | }
       """.stripMargin.trim
    } else {
      val samplerClass = classOf[BernoulliCellSampler[UnsafeRow]].getName
      val sampler = ctx.addMutableState(s"$samplerClass<UnsafeRow>", "sampler",
        v => s"""
          | $v = new $samplerClass<UnsafeRow>($lowerBound, $upperBound, false);
          | $v.setSeed(${seed}L + partitionIndex);
         """.stripMargin.trim)

      s"""
         | if ($sampler.sample() != 0) {
         |   $numOutput.add(1);
         |   ${consume(ctx, input)}
         | }
       """.stripMargin.trim
    }
  }
}


/**
 * Physical plan for range (generating a range of 64 bit numbers).
 */
case class RangeExec(range: org.apache.spark.sql.catalyst.plans.logical.Range)
  extends LeafExecNode with CodegenSupport {

  val start: Long = range.start
  val end: Long = range.end
  val step: Long = range.step
  val numSlices: Int = range.numSlices.getOrElse(sparkContext.defaultParallelism)
  val numElements: BigInt = range.numElements

  override val output: Seq[Attribute] = range.output

  override def outputOrdering: Seq[SortOrder] = range.outputOrdering

  override def outputPartitioning: Partitioning = {
    if (numElements > 0) {
      if (numSlices == 1) {
        SinglePartition
      } else {
        RangePartitioning(outputOrdering, numSlices)
      }
    } else {
      UnknownPartitioning(0)
    }
  }

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  override def doCanonicalize(): SparkPlan = {
    RangeExec(range.canonicalized.asInstanceOf[org.apache.spark.sql.catalyst.plans.logical.Range])
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    val rdd = if (start == end || (start < end ^ 0 < step)) {
      new EmptyRDD[InternalRow](sqlContext.sparkContext)
    } else {
      sqlContext.sparkContext.parallelize(0 until numSlices, numSlices).map(i => InternalRow(i))
    }
    rdd :: Nil
  }

  protected override def doProduce(ctx: CodegenContext): String = {
    val numOutput = metricTerm(ctx, "numOutputRows")

    val initTerm = ctx.addMutableState(CodeGenerator.JAVA_BOOLEAN, "initRange")
    val nextIndex = ctx.addMutableState(CodeGenerator.JAVA_LONG, "nextIndex")

    val value = ctx.freshName("value")
    val ev = ExprCode.forNonNullValue(JavaCode.variable(value, LongType))
    val BigInt = classOf[java.math.BigInteger].getName

    // Inline mutable state since not many Range operations in a task
    val taskContext = ctx.addMutableState("TaskContext", "taskContext",
      v => s"$v = TaskContext.get();", forceInline = true)
    val inputMetrics = ctx.addMutableState("InputMetrics", "inputMetrics",
      v => s"$v = $taskContext.taskMetrics().inputMetrics();", forceInline = true)

    // In order to periodically update the metrics without inflicting performance penalty, this
    // operator produces elements in batches. After a batch is complete, the metrics are updated
    // and a new batch is started.
    // In the implementation below, the code in the inner loop is producing all the values
    // within a batch, while the code in the outer loop is setting batch parameters and updating
    // the metrics.

    // Once nextIndex == batchEnd, it's time to progress to the next batch.
    val batchEnd = ctx.addMutableState(CodeGenerator.JAVA_LONG, "batchEnd")

    // How many values should still be generated by this range operator.
    val numElementsTodo = ctx.addMutableState(CodeGenerator.JAVA_LONG, "numElementsTodo")

    // How many values should be generated in the next batch.
    val nextBatchTodo = ctx.freshName("nextBatchTodo")

    // The default size of a batch, which must be positive integer
    val batchSize = 1000

    val initRangeFuncName = ctx.addNewFunction("initRange",
      s"""
        | private void initRange(int idx) {
        |   $BigInt index = $BigInt.valueOf(idx);
        |   $BigInt numSlice = $BigInt.valueOf(${numSlices}L);
        |   $BigInt numElement = $BigInt.valueOf(${numElements.toLong}L);
        |   $BigInt step = $BigInt.valueOf(${step}L);
        |   $BigInt start = $BigInt.valueOf(${start}L);
        |   long partitionEnd;
        |
        |   $BigInt st = index.multiply(numElement).divide(numSlice).multiply(step).add(start);
        |   if (st.compareTo($BigInt.valueOf(Long.MAX_VALUE)) > 0) {
        |     $nextIndex = Long.MAX_VALUE;
        |   } else if (st.compareTo($BigInt.valueOf(Long.MIN_VALUE)) < 0) {
        |     $nextIndex = Long.MIN_VALUE;
        |   } else {
        |     $nextIndex = st.longValue();
        |   }
        |   $batchEnd = $nextIndex;
        |
        |   $BigInt end = index.add($BigInt.ONE).multiply(numElement).divide(numSlice)
        |     .multiply(step).add(start);
        |   if (end.compareTo($BigInt.valueOf(Long.MAX_VALUE)) > 0) {
        |     partitionEnd = Long.MAX_VALUE;
        |   } else if (end.compareTo($BigInt.valueOf(Long.MIN_VALUE)) < 0) {
        |     partitionEnd = Long.MIN_VALUE;
        |   } else {
        |     partitionEnd = end.longValue();
        |   }
        |
        |   $BigInt startToEnd = $BigInt.valueOf(partitionEnd).subtract(
        |     $BigInt.valueOf($nextIndex));
        |   $numElementsTodo  = startToEnd.divide(step).longValue();
        |   if ($numElementsTodo < 0) {
        |     $numElementsTodo = 0;
        |   } else if (startToEnd.remainder(step).compareTo($BigInt.valueOf(0L)) != 0) {
        |     $numElementsTodo++;
        |   }
        | }
       """.stripMargin)

    val localIdx = ctx.freshName("localIdx")
    val localEnd = ctx.freshName("localEnd")
    val stopCheck = if (parent.needStopCheck) {
      s"""
         |if (shouldStop()) {
         |  $nextIndex = $value + ${step}L;
         |  $numOutput.add($localIdx + 1);
         |  $inputMetrics.incRecordsRead($localIdx + 1);
         |  return;
         |}
       """.stripMargin
    } else {
      "// shouldStop check is eliminated"
    }
    val loopCondition = if (limitNotReachedChecks.isEmpty) {
      "true"
    } else {
      limitNotReachedChecks.mkString(" && ")
    }

    // An overview of the Range processing.
    //
    // For each partition, the Range task needs to produce records from partition start(inclusive)
    // to end(exclusive). For better performance, we separate the partition range into batches, and
    // use 2 loops to produce data. The outer while loop is used to iterate batches, and the inner
    // for loop is used to iterate records inside a batch.
    //
    // `nextIndex` tracks the index of the next record that is going to be consumed, initialized
    // with partition start. `batchEnd` tracks the end index of the current batch, initialized
    // with `nextIndex`. In the outer loop, we first check if `nextIndex == batchEnd`. If it's true,
    // it means the current batch is fully consumed, and we will update `batchEnd` to process the
    // next batch. If `batchEnd` reaches partition end, exit the outer loop. Finally we enter the
    // inner loop. Note that, when we enter inner loop, `nextIndex` must be different from
    // `batchEnd`, otherwise we already exit the outer loop.
    //
    // The inner loop iterates from 0 to `localEnd`, which is calculated by
    // `(batchEnd - nextIndex) / step`. Since `batchEnd` is increased by `nextBatchTodo * step` in
    // the outer loop, and initialized with `nextIndex`, so `batchEnd - nextIndex` is always
    // divisible by `step`. The `nextIndex` is increased by `step` during each iteration, and ends
    // up being equal to `batchEnd` when the inner loop finishes.
    //
    // The inner loop can be interrupted, if the query has produced at least one result row, so that
    // we don't buffer too many result rows and waste memory. It's ok to interrupt the inner loop,
    // because `nextIndex` will be updated before interrupting.

    s"""
      | // initialize Range
      | if (!$initTerm) {
      |   $initTerm = true;
      |   $initRangeFuncName(partitionIndex);
      | }
      |
      | while ($loopCondition) {
      |   if ($nextIndex == $batchEnd) {
      |     long $nextBatchTodo;
      |     if ($numElementsTodo > ${batchSize}L) {
      |       $nextBatchTodo = ${batchSize}L;
      |       $numElementsTodo -= ${batchSize}L;
      |     } else {
      |       $nextBatchTodo = $numElementsTodo;
      |       $numElementsTodo = 0;
      |       if ($nextBatchTodo == 0) break;
      |     }
      |     $batchEnd += $nextBatchTodo * ${step}L;
      |   }
      |
      |   int $localEnd = (int)(($batchEnd - $nextIndex) / ${step}L);
      |   for (int $localIdx = 0; $localIdx < $localEnd; $localIdx++) {
      |     long $value = ((long)$localIdx * ${step}L) + $nextIndex;
      |     ${consume(ctx, Seq(ev))}
      |     $stopCheck
      |   }
      |   $nextIndex = $batchEnd;
      |   $numOutput.add($localEnd);
      |   $inputMetrics.incRecordsRead($localEnd);
      |   $taskContext.killTaskIfInterrupted();
      | }
     """.stripMargin
  }

  protected override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    sqlContext
      .sparkContext
      .parallelize(0 until numSlices, numSlices)
      .mapPartitionsWithIndex { (i, _) =>
        val partitionStart = (i * numElements) / numSlices * step + start
        val partitionEnd = (((i + 1) * numElements) / numSlices) * step + start
        def getSafeMargin(bi: BigInt): Long =
          if (bi.isValidLong) {
            bi.toLong
          } else if (bi > 0) {
            Long.MaxValue
          } else {
            Long.MinValue
          }
        val safePartitionStart = getSafeMargin(partitionStart)
        val safePartitionEnd = getSafeMargin(partitionEnd)
        val rowSize = UnsafeRow.calculateBitSetWidthInBytes(1) + LongType.defaultSize
        val unsafeRow = UnsafeRow.createFromByteArray(rowSize, 1)
        val taskContext = TaskContext.get()

        val iter = new Iterator[InternalRow] {
          private[this] var number: Long = safePartitionStart
          private[this] var overflow: Boolean = false
          private[this] val inputMetrics = taskContext.taskMetrics().inputMetrics

          override def hasNext =
            if (!overflow) {
              if (step > 0) {
                number < safePartitionEnd
              } else {
                number > safePartitionEnd
              }
            } else false

          override def next() = {
            val ret = number
            number += step
            if (number < ret ^ step < 0) {
              // we have Long.MaxValue + Long.MaxValue < Long.MaxValue
              // and Long.MinValue + Long.MinValue > Long.MinValue, so iff the step causes a step
              // back, we are pretty sure that we have an overflow.
              overflow = true
            }

            numOutputRows += 1
            inputMetrics.incRecordsRead(1)
            unsafeRow.setLong(0, ret)
            unsafeRow
          }
        }
        new InterruptibleIterator(taskContext, iter)
      }
  }

  override def simpleString(maxFields: Int): String = {
    s"Range ($start, $end, step=$step, splits=$numSlices)"
  }
}

/**
 * Physical plan for unioning two plans, without a distinct. This is UNION ALL in SQL.
 *
 * If we change how this is implemented physically, we'd need to update
 * [[org.apache.spark.sql.catalyst.plans.logical.Union.maxRowsPerPartition]].
 */
case class UnionExec(children: Seq[SparkPlan]) extends SparkPlan {
  // updating nullability to make all the children consistent
  override def output: Seq[Attribute] = {
    children.map(_.output).transpose.map { attrs =>
      val firstAttr = attrs.head
      val nullable = attrs.exists(_.nullable)
      val newDt = attrs.map(_.dataType).reduce(StructType.merge)
      if (firstAttr.dataType == newDt) {
        firstAttr.withNullability(nullable)
      } else {
        AttributeReference(firstAttr.name, newDt, nullable, firstAttr.metadata)(
          firstAttr.exprId, firstAttr.qualifier)
      }
    }
  }

  protected override def doExecute(): RDD[InternalRow] =
    sparkContext.union(children.map(_.execute()))
}

/**
 * Physical plan for returning a new RDD that has exactly `numPartitions` partitions.
 * Similar to coalesce defined on an [[RDD]], this operation results in a narrow dependency, e.g.
 * if you go from 1000 partitions to 100 partitions, there will not be a shuffle, instead each of
 * the 100 new partitions will claim 10 of the current partitions.  If a larger number of partitions
 * is requested, it will stay at the current number of partitions.
 *
 * However, if you're doing a drastic coalesce, e.g. to numPartitions = 1,
 * this may result in your computation taking place on fewer nodes than
 * you like (e.g. one node in the case of numPartitions = 1). To avoid this,
 * you see ShuffleExchange. This will add a shuffle step, but means the
 * current upstream partitions will be executed in parallel (per whatever
 * the current partitioning is).
 */
case class CoalesceExec(numPartitions: Int, child: SparkPlan) extends UnaryExecNode {
  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = {
    if (numPartitions == 1) SinglePartition
    else UnknownPartitioning(numPartitions)
  }

  protected override def doExecute(): RDD[InternalRow] = {
    if (numPartitions == 1 && child.execute().getNumPartitions < 1) {
      // Make sure we don't output an RDD with 0 partitions, when claiming that we have a
      // `SinglePartition`.
      new CoalesceExec.EmptyRDDWithPartitions(sparkContext, numPartitions)
    } else {
      child.execute().coalesce(numPartitions, shuffle = false)
    }
  }
}

object CoalesceExec {
  /** A simple RDD with no data, but with the given number of partitions. */
  class EmptyRDDWithPartitions(
      @transient private val sc: SparkContext,
      numPartitions: Int) extends RDD[InternalRow](sc, Nil) {

    override def getPartitions: Array[Partition] =
      Array.tabulate(numPartitions)(i => EmptyPartition(i))

    override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
      Iterator.empty
    }
  }

  case class EmptyPartition(index: Int) extends Partition
}

/**
 * Parent class for different types of subquery plans
 */
abstract class BaseSubqueryExec extends SparkPlan {
  def name: String
  def child: SparkPlan

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering
}

/**
 * Physical plan for a subquery.
 */
case class SubqueryExec(name: String, child: SparkPlan)
  extends BaseSubqueryExec with UnaryExecNode {

  override lazy val metrics = Map(
    "dataSize" -> SQLMetrics.createSizeMetric(sparkContext, "data size"),
    "collectTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to collect"))

  @transient
  private lazy val relationFuture: Future[Array[InternalRow]] = {
    // relationFuture is used in "doExecute". Therefore we can get the execution id correctly here.
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    Future {
      // This will run in another thread. Set the execution id so that we can connect these jobs
      // with the correct execution.
      SQLExecution.withExecutionId(sqlContext.sparkSession, executionId) {
        val beforeCollect = System.nanoTime()
        // Note that we use .executeCollect() because we don't want to convert data to Scala types
        val rows: Array[InternalRow] = child.executeCollect()
        val beforeBuild = System.nanoTime()
        longMetric("collectTime") += NANOSECONDS.toMillis(beforeBuild - beforeCollect)
        val dataSize = rows.map(_.asInstanceOf[UnsafeRow].getSizeInBytes.toLong).sum
        longMetric("dataSize") += dataSize

        SQLMetrics.postDriverMetricUpdates(sparkContext, executionId, metrics.values.toSeq)
        rows
      }
    }(SubqueryExec.executionContext)
  }

  protected override def doCanonicalize(): SparkPlan = {
    SubqueryExec("Subquery", child.canonicalized)
  }

  protected override def doPrepare(): Unit = {
    relationFuture
  }

  protected override def doExecute(): RDD[InternalRow] = {
    child.execute()
  }

  override def executeCollect(): Array[InternalRow] = {
    ThreadUtils.awaitResult(relationFuture, Duration.Inf)
  }

  override def stringArgs: Iterator[Any] = super.stringArgs ++ Iterator(s"[id=#$id]")
}

object SubqueryExec {
  private[execution] val executionContext = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonCachedThreadPool("subquery", 16))
}

/**
 * A wrapper for reused [[BaseSubqueryExec]].
 */
case class ReusedSubqueryExec(child: BaseSubqueryExec)
  extends BaseSubqueryExec with LeafExecNode {

  override def name: String = child.name

  override def output: Seq[Attribute] = child.output
  override def doCanonicalize(): SparkPlan = child.canonicalized
  override def outputOrdering: Seq[SortOrder] = child.outputOrdering
  override def outputPartitioning: Partitioning = child.outputPartitioning

  protected override def doPrepare(): Unit = child.prepare()

  protected override def doExecute(): RDD[InternalRow] = child.execute()

  override def executeCollect(): Array[InternalRow] = child.executeCollect()
}
