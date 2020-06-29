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

package org.apache.spark.sql.execution.reuse

import scala.collection.mutable.Map

import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{BaseSubqueryExec, ExecSubqueryExpression, ReusedSubqueryExec, SparkPlan}
import org.apache.spark.sql.execution.exchange.{Exchange, ReusedExchangeExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

/**
 * Find out duplicated exchanges and subqueries in the whole spark plan including subqueries, then
 * use the same exhange or subquery for all the references.
 */
case class ReuseExchangeAndSubquery(conf: SQLConf) extends Rule[SparkPlan] {

  private class ReuseMap[T <: SparkPlan] {
    // To avoid costly canonicalization of an exchange or a subquery:
    // - we use its schema first to check if it can be replaced to a reused one at all
    // - we insert it into the map of canonicalized plans only when at least 2 have the same schema
    private val map = Map[StructType, (T, Map[SparkPlan, T])]()

    def lookup(plan: T): T = {
      val (firstSameSchemaPlan, sameResultPlans) = map.getOrElseUpdate(plan.schema, plan -> Map())
      if (firstSameSchemaPlan.ne(plan)) {
        if (sameResultPlans.isEmpty) {
          sameResultPlans += firstSameSchemaPlan.canonicalized -> firstSameSchemaPlan
        }
        sameResultPlans.getOrElseUpdate(plan.canonicalized, plan)
      } else {
        plan
      }
    }

    def addOrElse(plan: T, f: T => SparkPlan): SparkPlan = {
      val cached = lookup(plan)
      if (cached.eq(plan)) {
        plan
      } else {
        f(cached)
      }
    }
  }

  def apply(plan: SparkPlan): SparkPlan = {
    if (conf.exchangeReuseEnabled || conf.subqueryReuseEnabled) {
      val exchanges = new ReuseMap[Exchange]()
      val subqueries = new ReuseMap[BaseSubqueryExec]()

      def reuse(plan: SparkPlan): SparkPlan = plan.transformUp {
        case exchange: Exchange if conf.exchangeReuseEnabled =>
          exchanges.addOrElse(exchange, ReusedExchangeExec(exchange.output, _))

        case other => other.transformExpressionsUp {
          case sub: ExecSubqueryExpression =>
            val subquery = reuse(sub.plan).asInstanceOf[BaseSubqueryExec]
            sub.withNewPlan(
              if (conf.subqueryReuseEnabled) {
                subqueries.addOrElse(subquery, ReusedSubqueryExec(_)).asInstanceOf[BaseSubqueryExec]
              } else {
                subquery
              }
            )
        }
      }

      reuse(plan)
    } else {
      plan
    }
  }
}
