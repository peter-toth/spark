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

package org.apache.spark.sql.execution.datasources

import scala.collection.mutable

import org.apache.spark.sql.catalyst.catalog.CatalogStatistics
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LeafNode, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.FilterEstimation
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * Prune the partitions of file source based table using partition filters. Currently, this rule
 * is applied to [[HadoopFsRelation]] with [[CatalogFileIndex]].
 *
 * For [[HadoopFsRelation]], the location will be replaced by pruned file index, and corresponding
 * statistics will be updated. And the partition filters will be kept in the filters of returned
 * logical plan.
 */
private[sql] object PruneFileSourcePartitions extends Rule[LogicalPlan] {

  private def rebuildPhysicalOperation(
      projects: Seq[NamedExpression],
      filters: Seq[Expression],
      relation: LeafNode): Project = {
    val withFilter = if (filters.nonEmpty) {
      val filterExpression = filters.reduceLeft(And)
      Filter(filterExpression, relation)
    } else {
      relation
    }
    Project(projects, withFilter)
  }

  private case class PartitionedLogicalRelation(
      logicalRelation: LogicalRelation,
      partitionKeyFilter: Expression) extends LeafNode {
    override def output: Seq[Attribute] = logicalRelation.output
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    // Collect the set of partition filters for different `CatalogFileIndex`es.
    // We will need to `Or` these filters when we call list partitions to get the union of
    // partitions we need.
    val catalogFileIndexFilters =
      mutable.Map.empty[CatalogFileIndex, ExpressionSet].withDefaultValue(ExpressionSet())
    val tempPlan = plan transformDown {
      case PhysicalOperation(
          projects,
          filters,
          logicalRelation @ LogicalRelation(
              fsRelation @ HadoopFsRelation(
                  catalogFileIndex: CatalogFileIndex,
                  partitionSchema,
                  _,
                  _,
                  _,
                  _),
              _,
              _,
              _))
          if fsRelation.partitionSchema.nonEmpty =>
        val partitionKeyFilter = if (filters.nonEmpty) {
          val normalizedFilters = DataSourceStrategy.normalizeExprs(
            filters.filter(f => !SubqueryExpression.hasSubquery(f) &&
              DataSourceUtils.shouldPushFilter(f, fsRelation.fileFormat.supportsCollationPushDown)),
            logicalRelation.output)
          val (partitionKeyFilters, _) = DataSourceUtils
            .getPartitionFiltersAndDataFilters(partitionSchema, normalizedFilters)

          if (partitionKeyFilters.nonEmpty) {
            partitionKeyFilters.reduce(And)
          } else {
            Literal.TrueLiteral
          }
        } else {
          Literal.TrueLiteral
        }

        catalogFileIndexFilters(catalogFileIndex) += partitionKeyFilter
        val partitionedLogicalRelation =
          PartitionedLogicalRelation(logicalRelation, partitionKeyFilter)
        rebuildPhysicalOperation(projects, filters, partitionedLogicalRelation)
    }

    if (catalogFileIndexFilters.isEmpty) {
      tempPlan
    } else {
      // Run partitions to get the union of partitions
      val tablePartitions = catalogFileIndexFilters.map {
        case (catalogFileIndex, partitionFilters) =>
          catalogFileIndex -> catalogFileIndex.listPartitions(Seq(partitionFilters.reduce(Or)))
      }

      // Build `InMemoryFileIndex`s from the union of partitions and partitioning filters used at
      // certain places
      tempPlan.transformDown {
        case PartitionedLogicalRelation(
            logicalRelation @ LogicalRelation(
                fsRelation @ HadoopFsRelation(catalogFileIndex: CatalogFileIndex, _, _, _, _, _),
                _,
                _,
                _),
            partitionKeyFilter) =>
          val (catalogTablePartitions, baseTimeNs) = tablePartitions(catalogFileIndex)
          val prunedFileIndex = catalogFileIndex.filterPartitions(catalogTablePartitions,
            baseTimeNs, Seq(partitionKeyFilter)) // TODO: What shall we do with baseTimeNs?
          val prunedFsRelation =
            fsRelation.copy(location = prunedFileIndex)(fsRelation.sparkSession)
          val filteredStats =
            FilterEstimation(Filter(partitionKeyFilter, logicalRelation)).estimate
          val colStats = filteredStats.map(_.attributeStats.map { case (attr, colStat) =>
            (attr.name, colStat.toCatalogColumnStat(attr.name, attr.dataType))
          })
          val withStats = logicalRelation.catalogTable.map(_.copy(
            stats = Some(CatalogStatistics(
              sizeInBytes = BigInt(prunedFileIndex.sizeInBytes),
              rowCount = filteredStats.flatMap(_.rowCount),
              colStats = colStats.getOrElse(Map.empty)))))
          logicalRelation.copy(
            relation = prunedFsRelation, catalogTable = withStats)
      }
    }
  }
}
