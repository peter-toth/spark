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

package org.apache.spark.sql.catalyst.optimizer

import scala.annotation.tailrec

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.{ExtractEquiJoinKeys, ExtractFiltersAndInnerJoins}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.internal.SQLConf

/**
 * Reorder the joins and push all the conditions into join, so that the bottom ones have at least
 * one condition.
 *
 * The order of joins will not be changed if all of them already have at least one condition.
 *
 * If star schema detection is enabled, reorder the star join plans based on heuristics.
 */
object ReorderJoin extends Rule[LogicalPlan] with PredicateHelper {
  /**
   * Join a list of plans together and push down the conditions into them.
   *
   * The joined plan are picked from left to right, prefer those has at least one join condition.
   *
   * @param input a list of LogicalPlans to inner join and the type of inner join.
   * @param conditions a list of condition for join.
   */
  @tailrec
  final def createOrderedJoin(
      input: Seq[(LogicalPlan, InnerLike)],
      conditions: Seq[Expression]): LogicalPlan = {
    assert(input.size >= 2)
    if (input.size == 2) {
      val (joinConditions, others) = conditions.partition(canEvaluateWithinJoin)
      val ((left, leftJoinType), (right, rightJoinType)) = (input(0), input(1))
      val innerJoinType = (leftJoinType, rightJoinType) match {
        case (Inner, Inner) => Inner
        case (_, _) => Cross
      }
      val join = Join(left, right, innerJoinType,
        joinConditions.reduceLeftOption(And), JoinHint.NONE)
      if (others.nonEmpty) {
        Filter(others.reduceLeft(And), join)
      } else {
        join
      }
    } else {
      val (left, _) :: rest = input.toList
      // find out the first join that have at least one join condition
      val conditionalJoin = rest.find { planJoinPair =>
        val plan = planJoinPair._1
        val refs = left.outputSet ++ plan.outputSet
        conditions
          .filterNot(l => l.references.nonEmpty && canEvaluate(l, left))
          .filterNot(r => r.references.nonEmpty && canEvaluate(r, plan))
          .exists(_.references.subsetOf(refs))
      }
      // pick the next one if no condition left
      val (right, innerJoinType) = conditionalJoin.getOrElse(rest.head)

      val joinedRefs = left.outputSet ++ right.outputSet
      val (joinConditions, others) = conditions.partition(
        e => e.references.subsetOf(joinedRefs) && canEvaluateWithinJoin(e))
      val joined = Join(left, right, innerJoinType,
        joinConditions.reduceLeftOption(And), JoinHint.NONE)

      // should not have reference to same logical plan
      createOrderedJoin(Seq((joined, Inner)) ++ rest.filterNot(_._1 eq right), others)
    }
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case p @ ExtractFiltersAndInnerJoins(input, conditions)
        if input.size > 2 && conditions.nonEmpty =>
      val reordered = if (SQLConf.get.starSchemaDetection && !SQLConf.get.cboEnabled) {
        val starJoinPlan = StarSchemaDetection.reorderStarJoins(input, conditions)
        if (starJoinPlan.nonEmpty) {
          val rest = input.filterNot(starJoinPlan.contains(_))
          createOrderedJoin(starJoinPlan ++ rest, conditions)
        } else {
          createOrderedJoin(input, conditions)
        }
      } else {
        createOrderedJoin(input, conditions)
      }

      if (p.sameOutput(reordered)) {
        reordered
      } else {
        // Reordering the joins have changed the order of the columns.
        // Inject a projection to make sure we restore to the expected ordering.
        Project(p.output, reordered)
      }
  }
}

/**
 * Elimination of outer joins, if the predicates can restrict the result sets so that
 * all null-supplying rows are eliminated
 *
 * - full outer -> inner if both sides have such predicates
 * - left outer -> inner if the right side has such predicates
 * - right outer -> inner if the left side has such predicates
 * - full outer -> left outer if only the left side has such predicates
 * - full outer -> right outer if only the right side has such predicates
 *
 * This rule should be executed before pushing down the Filter
 */
object EliminateOuterJoin extends Rule[LogicalPlan] with PredicateHelper {

  /**
   * Returns whether the expression returns null or false when all inputs are nulls.
   */
  private def canFilterOutNull(e: Expression): Boolean = {
    if (!e.deterministic || SubqueryExpression.hasCorrelatedSubquery(e)) return false
    val attributes = e.references.toSeq
    val emptyRow = new GenericInternalRow(attributes.length)
    val boundE = BindReferences.bindReference(e, attributes)
    if (boundE.find(_.isInstanceOf[Unevaluable]).isDefined) return false
    val v = boundE.eval(emptyRow)
    v == null || v == false
  }

  private def buildNewJoinType(filter: Filter, join: Join): JoinType = {
    val conditions = splitConjunctivePredicates(filter.condition) ++ filter.constraints
    val leftConditions = conditions.filter(_.references.subsetOf(join.left.outputSet))
    val rightConditions = conditions.filter(_.references.subsetOf(join.right.outputSet))

    lazy val leftHasNonNullPredicate = leftConditions.exists(canFilterOutNull)
    lazy val rightHasNonNullPredicate = rightConditions.exists(canFilterOutNull)

    join.joinType match {
      case RightOuter if leftHasNonNullPredicate => Inner
      case LeftOuter if rightHasNonNullPredicate => Inner
      case FullOuter if leftHasNonNullPredicate && rightHasNonNullPredicate => Inner
      case FullOuter if leftHasNonNullPredicate => LeftOuter
      case FullOuter if rightHasNonNullPredicate => RightOuter
      case o => o
    }
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case f @ Filter(condition, j @ Join(_, _, RightOuter | LeftOuter | FullOuter, _, _)) =>
      val newJoinType = buildNewJoinType(f, j)
      if (j.joinType == newJoinType) f else Filter(condition, j.copy(joinType = newJoinType))
  }
}

/**
 * PythonUDF in join condition can't be evaluated if it refers to attributes from both join sides.
 * See `ExtractPythonUDFs` for details. This rule will detect un-evaluable PythonUDF and pull them
 * out from join condition.
 */
object ExtractPythonUDFFromJoinCondition extends Rule[LogicalPlan] with PredicateHelper {

  private def hasUnevaluablePythonUDF(expr: Expression, j: Join): Boolean = {
    expr.find { e =>
      PythonUDF.isScalarPythonUDF(e) && !canEvaluate(e, j.left) && !canEvaluate(e, j.right)
    }.isDefined
  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    case j @ Join(_, _, joinType, Some(cond), _) if hasUnevaluablePythonUDF(cond, j) =>
      if (!joinType.isInstanceOf[InnerLike]) {
        // The current strategy supports only InnerLike join because for other types,
        // it breaks SQL semantic if we run the join condition as a filter after join. If we pass
        // the plan here, it'll still get a an invalid PythonUDF RuntimeException with message
        // `requires attributes from more than one child`, we throw firstly here for better
        // readable information.
        throw new AnalysisException("Using PythonUDF in join condition of join type" +
          s" $joinType is not supported.")
      }
      // If condition expression contains python udf, it will be moved out from
      // the new join conditions.
      val (udf, rest) = splitConjunctivePredicates(cond).partition(hasUnevaluablePythonUDF(_, j))
      val newCondition = if (rest.isEmpty) {
        logWarning(s"The join condition:$cond of the join plan contains PythonUDF only," +
          s" it will be moved out and the join plan will be turned to cross join.")
        None
      } else {
        Some(rest.reduceLeft(And))
      }
      val newJoin = j.copy(condition = newCondition)
      joinType match {
        case _: InnerLike => Filter(udf.reduceLeft(And), newJoin)
        case _ =>
          throw new AnalysisException("Using PythonUDF in join condition of join type" +
            s" $joinType is not supported.")
      }
  }
}

sealed abstract class BuildSide

case object BuildRight extends BuildSide

case object BuildLeft extends BuildSide

trait JoinSelectionHelper {

  def getBroadcastBuildSide(
      left: LogicalPlan,
      right: LogicalPlan,
      joinType: JoinType,
      hint: JoinHint,
      hintOnly: Boolean,
      conf: SQLConf): Option[BuildSide] = {
    val buildLeft = if (hintOnly) {
      hintToBroadcastLeft(hint)
    } else {
      canBroadcastBySize(left, conf) && !hintToNotBroadcastLeft(hint)
    }
    val buildRight = if (hintOnly) {
      hintToBroadcastRight(hint)
    } else {
      canBroadcastBySize(right, conf) && !hintToNotBroadcastRight(hint)
    }
    getBuildSide(
      canBuildLeft(joinType) && buildLeft,
      canBuildRight(joinType) && buildRight,
      left,
      right
    )
  }

  def getShuffleHashJoinBuildSide(
      left: LogicalPlan,
      right: LogicalPlan,
      joinType: JoinType,
      hint: JoinHint,
      hintOnly: Boolean,
      conf: SQLConf): Option[BuildSide] = {
    val buildLeft = if (hintOnly) {
      hintToShuffleHashJoinLeft(hint)
    } else {
      canBuildLocalHashMapBySize(left, conf) && muchSmaller(left, right)
    }
    val buildRight = if (hintOnly) {
      hintToShuffleHashJoinRight(hint)
    } else {
      canBuildLocalHashMapBySize(right, conf) && muchSmaller(right, left)
    }
    getBuildSide(
      canBuildLeft(joinType) && buildLeft,
      canBuildRight(joinType) && buildRight,
      left,
      right
    )
  }

  def getSmallerSide(left: LogicalPlan, right: LogicalPlan): BuildSide = {
    if (right.stats.sizeInBytes <= left.stats.sizeInBytes) BuildRight else BuildLeft
  }

  /**
   * Matches a plan whose output should be small enough to be used in broadcast join.
   */
  def canBroadcastBySize(plan: LogicalPlan, conf: SQLConf): Boolean = {
    plan.stats.sizeInBytes >= 0 && plan.stats.sizeInBytes <= conf.autoBroadcastJoinThreshold
  }

  def canBuildLeft(joinType: JoinType): Boolean = {
    joinType match {
      case _: InnerLike | RightOuter => true
      case _ => false
    }
  }

  def canBuildRight(joinType: JoinType): Boolean = {
    joinType match {
      case _: InnerLike | LeftOuter | LeftSemi | LeftAnti | _: ExistenceJoin => true
      case _ => false
    }
  }

  def hintToBroadcastLeft(hint: JoinHint): Boolean = {
    hint.leftHint.exists(_.strategy.contains(BROADCAST))
  }

  def hintToBroadcastRight(hint: JoinHint): Boolean = {
    hint.rightHint.exists(_.strategy.contains(BROADCAST))
  }

  def hintToNotBroadcastLeft(hint: JoinHint): Boolean = {
    hint.leftHint.exists(_.strategy.contains(NO_BROADCAST_HASH))
  }

  def hintToNotBroadcastRight(hint: JoinHint): Boolean = {
    hint.rightHint.exists(_.strategy.contains(NO_BROADCAST_HASH))
  }

  def hintToShuffleHashJoinLeft(hint: JoinHint): Boolean = {
    hint.leftHint.exists(_.strategy.contains(SHUFFLE_HASH))
  }

  def hintToShuffleHashJoinRight(hint: JoinHint): Boolean = {
    hint.rightHint.exists(_.strategy.contains(SHUFFLE_HASH))
  }

  def hintToSortMergeJoin(hint: JoinHint): Boolean = {
    hint.leftHint.exists(_.strategy.contains(SHUFFLE_MERGE)) ||
      hint.rightHint.exists(_.strategy.contains(SHUFFLE_MERGE))
  }

  def hintToShuffleReplicateNL(hint: JoinHint): Boolean = {
    hint.leftHint.exists(_.strategy.contains(SHUFFLE_REPLICATE_NL)) ||
      hint.rightHint.exists(_.strategy.contains(SHUFFLE_REPLICATE_NL))
  }

  private def getBuildSide(
      canBuildLeft: Boolean,
      canBuildRight: Boolean,
      left: LogicalPlan,
      right: LogicalPlan): Option[BuildSide] = {
    if (canBuildLeft && canBuildRight) {
      // returns the smaller side base on its estimated physical size, if we want to build the
      // both sides.
      Some(getSmallerSide(left, right))
    } else if (canBuildLeft) {
      Some(BuildLeft)
    } else if (canBuildRight) {
      Some(BuildRight)
    } else {
      None
    }
  }

  /**
   * Matches a plan whose single partition should be small enough to build a hash table.
   *
   * Note: this assume that the number of partition is fixed, requires additional work if it's
   * dynamic.
   */
  private def canBuildLocalHashMapBySize(plan: LogicalPlan, conf: SQLConf): Boolean = {
    plan.stats.sizeInBytes < conf.autoBroadcastJoinThreshold * conf.numShufflePartitions
  }

  /**
   * Returns whether plan a is much smaller (3X) than plan b.
   *
   * The cost to build hash map is higher than sorting, we should only build hash map on a table
   * that is much smaller than other one. Since we does not have the statistic for number of rows,
   * use the size of bytes here as estimation.
   */
  private def muchSmaller(a: LogicalPlan, b: LogicalPlan): Boolean = {
    a.stats.sizeInBytes * 3 <= b.stats.sizeInBytes
  }

  sealed trait JoinImplementationType {
    val id: Int
  }
  case class BroadcastHashJoin(
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      joinType: JoinType,
      buildSide: BuildSide,
      condition: Option[Expression],
      left: LogicalPlan,
      right: LogicalPlan) extends JoinImplementationType {
    override val id = 0
  }
  case class ShuffledHashJoin(
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      joinType: JoinType,
      buildSide: BuildSide,
      condition: Option[Expression],
      left: LogicalPlan,
      right: LogicalPlan) extends JoinImplementationType {
    override val id = 1
  }
  case class SortMergeJoin(
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      joinType: JoinType,
      condition: Option[Expression],
      left: LogicalPlan,
      right: LogicalPlan) extends JoinImplementationType {
    override val id = 2
  }
  case class BroadcastNestedLoopJoin(
      left: LogicalPlan,
      right: LogicalPlan,
      buildSide: BuildSide,
      joinType: JoinType,
      condition: Option[Expression]) extends JoinImplementationType {
    override val id = 3
  }
  case class CartesianProduct(
      left: LogicalPlan,
      right: LogicalPlan,
      condition: Option[Expression]) extends JoinImplementationType {
    override val id = 4
  }

  /**
   * Select the proper physical plan for join based on join strategy hints, the availability of
   * equi-join keys and the sizes of joining relations. Below are the existing join strategies,
   * their characteristics and their limitations.
   *
   * - Broadcast hash join (BHJ):
   *     Only supported for equi-joins, while the join keys do not need to be sortable.
   *     Supported for all join types except full outer joins.
   *     BHJ usually performs faster than the other join algorithms when the broadcast side is
   *     small. However, broadcasting tables is a network-intensive operation and it could cause
   *     OOM or perform badly in some cases, especially when the build/broadcast side is big.
   *
   * - Shuffle hash join:
   *     Only supported for equi-joins, while the join keys do not need to be sortable.
   *     Supported for all join types except full outer joins.
   *
   * - Shuffle sort merge join (SMJ):
   *     Only supported for equi-joins and the join keys have to be sortable.
   *     Supported for all join types.
   *
   * - Broadcast nested loop join (BNLJ):
   *     Supports both equi-joins and non-equi-joins.
   *     Supports all the join types, but the implementation is optimized for:
   *       1) broadcasting the left side in a right outer join;
   *       2) broadcasting the right side in a left outer, left semi, left anti or existence join;
   *       3) broadcasting either side in an inner-like join.
   *     For other cases, we need to scan the data multiple times, which can be rather slow.
   *
   * - Shuffle-and-replicate nested loop join (a.k.a. cartesian product join):
   *     Supports both equi-joins and non-equi-joins.
   *     Supports only inner like joins.
   */
  def selectJoin(plan: Join, conf: SQLConf): JoinImplementationType = plan match {
    // If it is an equi-join, we first look at the join hints w.r.t. the following order:
    //   1. broadcast hint: pick broadcast hash join if the join type is supported. If both sides
    //      have the broadcast hints, choose the smaller side (based on stats) to broadcast.
    //   2. sort merge hint: pick sort merge join if join keys are sortable.
    //   3. shuffle hash hint: We pick shuffle hash join if the join type is supported. If both
    //      sides have the shuffle hash hints, choose the smaller side (based on stats) as the
    //      build side.
    //   4. shuffle replicate NL hint: pick cartesian product if join type is inner like.
    //
    // If there is no hint or the hints are not applicable, we follow these rules one by one:
    //   1. Pick broadcast hash join if one side is small enough to broadcast, and the join type
    //      is supported. If both sides are small, choose the smaller side (based on stats)
    //      to broadcast.
    //   2. Pick shuffle hash join if one side is small enough to build local hash map, and is
    //      much smaller than the other side, and `spark.sql.join.preferSortMergeJoin` is false.
    //   3. Pick sort merge join if the join keys are sortable.
    //   4. Pick cartesian product if join type is inner like.
    //   5. Pick broadcast nested loop join as the final solution. It may OOM but we don't have
    //      other choice.
    case ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, condition, left, right, hint) =>
      def createBroadcastHashJoin(onlyLookingAtHint: Boolean) = {
        getBroadcastBuildSide(left, right, joinType, hint, onlyLookingAtHint, conf).map {
          buildSide =>
            BroadcastHashJoin(leftKeys, rightKeys, joinType, buildSide, condition, left, right)
        }
      }

      def createShuffleHashJoin(onlyLookingAtHint: Boolean) = {
        getShuffleHashJoinBuildSide(left, right, joinType, hint, onlyLookingAtHint, conf).map {
          buildSide =>
            ShuffledHashJoin(leftKeys, rightKeys, joinType, buildSide, condition, left, right)
        }
      }

      def createSortMergeJoin() = {
        if (RowOrdering.isOrderable(leftKeys)) {
          Some(SortMergeJoin(leftKeys, rightKeys, joinType, condition, left, right))
        } else {
          None
        }
      }

      def createCartesianProduct() = {
        if (joinType.isInstanceOf[InnerLike]) {
          Some(CartesianProduct(left, right, condition))
        } else {
          None
        }
      }

      def createJoinWithoutHint() = {
        createBroadcastHashJoin(false)
          .orElse {
            if (!conf.preferSortMergeJoin) {
              createShuffleHashJoin(false)
            } else {
              None
            }
          }
          .orElse(createSortMergeJoin())
          .orElse(createCartesianProduct())
          .getOrElse {
            // This join could be very slow or OOM
            val buildSide = getSmallerSide(left, right)
            BroadcastNestedLoopJoin(left, right, buildSide, joinType, condition)
          }
      }

      createBroadcastHashJoin(true)
        .orElse { if (hintToSortMergeJoin(hint)) createSortMergeJoin() else None }
        .orElse(createShuffleHashJoin(true))
        .orElse { if (hintToShuffleReplicateNL(hint)) createCartesianProduct() else None }
        .getOrElse(createJoinWithoutHint())

    // If it is not an equi-join, we first look at the join hints w.r.t. the following order:
    //   1. broadcast hint: pick broadcast nested loop join. If both sides have the broadcast
    //      hints, choose the smaller side (based on stats) to broadcast for inner and full joins,
    //      choose the left side for right join, and choose right side for left join.
    //   2. shuffle replicate NL hint: pick cartesian product if join type is inner like.
    //
    // If there is no hint or the hints are not applicable, we follow these rules one by one:
    //   1. Pick broadcast nested loop join if one side is small enough to broadcast. If only left
    //      side is broadcast-able and it's left join, or only right side is broadcast-able and
    //      it's right join, we skip this rule. If both sides are small, broadcasts the smaller
    //      side for inner and full joins, broadcasts the left side for right join, and broadcasts
    //      right side for left join.
    //   2. Pick cartesian product if join type is inner like.
    //   3. Pick broadcast nested loop join as the final solution. It may OOM but we don't have
    //      other choice. It broadcasts the smaller side for inner and full joins, broadcasts the
    //      left side for right join, and broadcasts right side for left join.
    case Join(left, right, joinType, condition, hint) =>
      val desiredBuildSide = if (joinType.isInstanceOf[InnerLike] || joinType == FullOuter) {
        getSmallerSide(left, right)
      } else {
        // For perf reasons, `BroadcastNestedLoopJoinExec` prefers to broadcast left side if
        // it's a right join, and broadcast right side if it's a left join.
        // TODO: revisit it. If left side is much smaller than the right side, it may be better
        // to broadcast the left side even if it's a left join.
        if (canBuildLeft(joinType)) BuildLeft else BuildRight
      }

      def createBroadcastNLJoin(buildLeft: Boolean, buildRight: Boolean) = {
        val maybeBuildSide = if (buildLeft && buildRight) {
          Some(desiredBuildSide)
        } else if (buildLeft) {
          Some(BuildLeft)
        } else if (buildRight) {
          Some(BuildRight)
        } else {
          None
        }

        maybeBuildSide.map { buildSide =>
          BroadcastNestedLoopJoin(left, right, buildSide, joinType, condition)
        }
      }

      def createCartesianProduct() = {
        if (joinType.isInstanceOf[InnerLike]) {
          Some(CartesianProduct(left, right, condition))
        } else {
          None
        }
      }

      def createJoinWithoutHint() = {
        createBroadcastNLJoin(canBroadcastBySize(left, conf), canBroadcastBySize(right, conf))
          .orElse(createCartesianProduct())
          .getOrElse {
            // This join could be very slow or OOM
            BroadcastNestedLoopJoin(left, right, desiredBuildSide, joinType, condition)
          }
      }

      createBroadcastNLJoin(hintToBroadcastLeft(hint), hintToBroadcastRight(hint))
        .orElse { if (hintToShuffleReplicateNL(hint)) createCartesianProduct() else None }
        .getOrElse(createJoinWithoutHint())
  }
}
