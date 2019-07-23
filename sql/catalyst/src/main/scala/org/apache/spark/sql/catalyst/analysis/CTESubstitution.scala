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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions.SubqueryExpression
import org.apache.spark.sql.catalyst.plans.logical.{Distinct, Except, LogicalPlan, RecursiveTable, SubqueryAlias, Union, With}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.LEGACY_CTE_PRECEDENCE_ENABLED

/**
 * Analyze WITH nodes and substitute child plan with CTE definitions.
 */
object CTESubstitution extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = {
    if (SQLConf.get.getConf(LEGACY_CTE_PRECEDENCE_ENABLED)) {
      legacyTraverseAndSubstituteCTE(plan)
    } else {
      traverseAndSubstituteCTE(plan, false)
    }
  }

  private def legacyTraverseAndSubstituteCTE(plan: LogicalPlan): LogicalPlan = {
    plan.resolveOperatorsUp {
      case With(child, relations, allowRecursion) =>
        // substitute CTE expressions right-to-left to resolve references to previous CTEs:
        // with a as (select * from t), b as (select * from a) select * from b
        relations.foldRight(child) {
          case ((cteName, ctePlan), currentPlan) =>
            val recursionHandledPlan = handleRecursion(ctePlan, cteName, allowRecursion)
            substituteCTE(currentPlan, cteName, recursionHandledPlan)
        }
    }
  }

  /**
   * Traverse the plan and expression nodes as a tree and replace matching references to CTE
   * definitions.
   * - If the rule encounters a WITH node then it substitutes the child of the node with CTE
   *   definitions of the node right-to-left order as a definition can reference to a previous
   *   one.
   *   For example the following query is valid:
   *   WITH
   *     t AS (SELECT 1),
   *     t2 AS (SELECT * FROM t)
   *   SELECT * FROM t2
   * - If a CTE definition contains an inner WITH node then substitution of inner should take
   *   precedence because it can shadow an outer CTE definition.
   *   For example the following query should return 2:
   *   WITH
   *     t AS (SELECT 1),
   *     t2 AS (
   *       WITH t AS (SELECT 2)
   *       SELECT * FROM t
   *     )
   *   SELECT * FROM t2
   * - If a CTE definition contains a subquery that contains an inner WITH node then substitution
   *   of inner should take precedence because it can shadow an outer CTE definition.
   *   For example the following query should return 2:
   *   WITH t AS (SELECT 1 AS c)
   *   SELECT max(c) FROM (
   *     WITH t AS (SELECT 2 AS c)
   *     SELECT * FROM t
   *   )
   * - If a CTE definition contains a subquery expression that contains an inner WITH node then
   *   substitution of inner should take precedence because it can shadow an outer CTE
   *   definition.
   *   For example the following query should return 2:
   *   WITH t AS (SELECT 1)
   *   SELECT (
   *     WITH t AS (SELECT 2)
   *     SELECT * FROM t
   *   )
   * @param plan the plan to be traversed
   * @param inTraverse whether the current traverse is called from another traverse, only in this
   *                   case name collision can occur
   * @return the plan where CTE substitution is applied
   */
  private def traverseAndSubstituteCTE(plan: LogicalPlan, inTraverse: Boolean): LogicalPlan = {
    plan.resolveOperatorsUp {
      case With(child: LogicalPlan, relations, allowRecursion) =>
        // child might contain an inner CTE that has priority so traverse and substitute inner CTEs
        // in child first
        val traversedChild: LogicalPlan = child transformExpressions {
          case e: SubqueryExpression => e.withNewPlan(traverseAndSubstituteCTE(e.plan, true))
        }

        // Substitute CTE definitions from last to first as a CTE definition can reference a
        // previous one
        relations.foldRight(traversedChild) {
          case ((cteName, ctePlan), currentPlan) =>
            // A CTE definition might contain an inner CTE that has priority, so traverse and
            // substitute CTE defined in ctePlan.
            // A CTE definition might not be used at all or might be used multiple times. To avoid
            // computation if it is not used and to avoid multiple recomputation if it is used
            // multiple times we use a lazy construct with call-by-name parameter passing.
            lazy val substitutedCTEPlan = traverseAndSubstituteCTE(ctePlan, true)
            lazy val recursionHandledPlan =
              handleRecursion(substitutedCTEPlan, cteName, allowRecursion)
            substituteCTE(currentPlan, cteName, recursionHandledPlan)
        }

      // CTE name collision can occur only when inTraverse is true, it helps to avoid eager CTE
      // substitution in a subquery expression.
      case other if inTraverse =>
        other.transformExpressions {
          case e: SubqueryExpression => e.withNewPlan(traverseAndSubstituteCTE(e.plan, true))
        }
    }
  }

  /**
   * If recursion is allowed recursion handling starts with inserting unresolved self-references
   * ([[UnresolvedRecursiveReference]]) to places where a reference to the CTE definition itself is
   * found.
   * If there is a self-reference then we need to check if structure of the query satisfies the SQL
   * recursion rules and determine if UNION or UNION ALL operator is used as combinator between the
   * terms and insert the appropriate [[RecursiveTable]] finally.
   */
  private def handleRecursion(
      plan: => LogicalPlan,
      recursiveTableName: String,
      allowRecursion: Boolean): LogicalPlan = {
    if (allowRecursion) {
      val (recursiveReferencesPlan, recursiveReferenceFound) =
        insertRecursiveReferences(plan, recursiveTableName)
      if (recursiveReferenceFound) {
        recursiveReferencesPlan match {
          case SubqueryAlias(_, u: Union) =>
            insertRecursiveTable(recursiveTableName, Seq.empty, false, u)
          case SubqueryAlias(_, Distinct(u: Union)) =>
            insertRecursiveTable(recursiveTableName, Seq.empty, true, u)
          case SubqueryAlias(_, UnresolvedSubqueryColumnAliases(columnNames, u: Union)) =>
            insertRecursiveTable(recursiveTableName, columnNames, false, u)
          case SubqueryAlias(_, UnresolvedSubqueryColumnAliases(columnNames, Distinct(u: Union))) =>
            insertRecursiveTable(recursiveTableName, columnNames, true, u)
          case _ =>
            throw new AnalysisException(s"Recursive query ${recursiveTableName} should contain " +
              s"UNION or UNION ALL statements only. This error can also be caused by ORDER BY or " +
              s"LIMIT keywords used on result of UNION or UNION ALL.")
        }
      } else {
        plan
      }
    } else {
      plan
    }
  }

  private def insertRecursiveReferences(
      plan: LogicalPlan,
      recursiveTableName: String): (LogicalPlan, Boolean) = {
    var recursiveReferenceFound = false

    val newPlan = plan resolveOperatorsUp {
      case u @ UnresolvedRelation(Seq(table)) if (plan.conf.resolver(recursiveTableName, table)) =>
        recursiveReferenceFound = true
        UnresolvedRecursiveReference(recursiveTableName, false)

      case other =>
        other transformExpressions {
          case e: SubqueryExpression =>
            val (substitutedPlan, recursiveReferenceFoundInSubQuery) =
              insertRecursiveReferences(e.plan, recursiveTableName)
            recursiveReferenceFound |= recursiveReferenceFoundInSubQuery
            e.withNewPlan(substitutedPlan)
        }
    }

    (newPlan, recursiveReferenceFound)
  }

  private def insertRecursiveTable(
      recursiveTableName: String,
      columnNames: Seq[String],
      distinct: Boolean,
      union: Union): LogicalPlan = {
    val terms = combineUnions(union, distinct)
    val (anchorTerms, recursiveTerms) = terms.partition(!_.collectFirst {
      case UnresolvedRecursiveReference(name, false) if name == recursiveTableName => true
    }.isDefined)

    if (anchorTerms.isEmpty) {
      throw new AnalysisException("There should be at least one anchor term defined in the " +
        s"recursive query ${recursiveTableName}")
    }

    // The first anchor has a special role, its output column are aliased if required.
    val firstAnchor = SubqueryAlias(
      recursiveTableName,
      if (columnNames.isEmpty) {
        anchorTerms.head
      } else {
        UnresolvedSubqueryColumnAliases(columnNames, anchorTerms.head)
      })

    // If UNION combinator is used between the terms we extend each of them (except for the first
    // anchor) with an EXCEPT clause and a reference to the so far cumulated result. If there are
    // multiple anchors defined then only the first anchor term remains anchor, the rest of the
    // terms becomes recursive due to the newly inserted recursive reference.
    val (distinctHandledAnchorTerms, distinctHandledRecursiveTerms) = if (distinct) {
      (Seq(Distinct(firstAnchor)), (anchorTerms.tail ++ recursiveTerms).map(
        Except(_, UnresolvedRecursiveReference(recursiveTableName, true), false)
      ))
    } else {
      (firstAnchor +: anchorTerms.tail, recursiveTerms)
    }

    RecursiveTable(recursiveTableName, distinctHandledAnchorTerms, distinctHandledRecursiveTerms,
      None)
  }

  private def combineUnions(union: Union, distinct: Boolean): Seq[LogicalPlan] = {
    union.children.flatMap {
      case Distinct(u: Union) if distinct => combineUnions(u, true)
      case u: Union if !distinct => combineUnions(u, false)
      case o => Seq(o)
    }
  }

  private def substituteCTE(
      plan: LogicalPlan,
      cteName: String,
      ctePlan: => LogicalPlan): LogicalPlan =
    plan resolveOperatorsUp {
      case UnresolvedRelation(Seq(table)) if plan.conf.resolver(cteName, table) => ctePlan

      case other =>
        // This cannot be done in ResolveSubquery because ResolveSubquery does not know the CTE.
        other transformExpressions {
          case e: SubqueryExpression => e.withNewPlan(substituteCTE(e.plan, cteName, ctePlan))
        }
    }
}
