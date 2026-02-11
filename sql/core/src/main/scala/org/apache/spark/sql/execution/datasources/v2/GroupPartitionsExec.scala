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


package org.apache.spark.sql.execution.datasources.v2

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.Partition
import org.apache.spark.rdd.{CoalescedRDD, PartitionCoalescer, PartitionGroup, RDD}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.{KeyedPartitioning, Partitioning}
import org.apache.spark.sql.catalyst.util.InternalRowComparableWrapper
import org.apache.spark.sql.connector.catalog.functions.Reducer
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.types.DataType

case class GroupPartitionsExec(
    child: SparkPlan,
    joinKeyPositions: Option[Seq[Int]] = None,
    commonPartitionValues: Option[Seq[(InternalRow, Int)]] = None,
    reducers: Option[Seq[Option[Reducer[_, _]]]] = None,
    applyPartialClustering: Boolean = false,
    replicatePartitions: Boolean = false
  ) extends UnaryExecNode {

  override def outputPartitioning: Partitioning = {
    child.outputPartitioning match {
      case p: Partitioning with Expression =>
        p.transform {
          case k: KeyedPartitioning =>
            val projectedExpressions = projectExpressions(k.expressions)
            val projectedDataTypes = projectedExpressions.map(_.dataType)
            k.copy(expressions = projectedExpressions,
              partitionKeys = groupedPartitions.map(_._1),
              originalPartitionKeys = projectKeys(k.originalPartitionKeys, projectedDataTypes))
        }.asInstanceOf[Partitioning]
      case o => o
    }
  }

  private def projectExpressions(expressions: Seq[Expression]) = {
    joinKeyPositions match {
      case Some(projectionPositions) =>
        projectionPositions.map(expressions)
      case _ => expressions
    }
  }

  private def projectKeys(keys: Seq[InternalRow], dataTypes: Seq[DataType]) = {
    joinKeyPositions match {
      case Some(projectionPositions) =>
        keys.map(KeyedPartitioning.projectKey(_, projectionPositions, dataTypes))
      case _ => keys
    }
  }

  lazy val firstKeyedPartitioning = {
    child.outputPartitioning.asInstanceOf[Partitioning with Expression].collectFirst {
      case k: KeyedPartitioning => k
    }.get
  }

  lazy val groupedPartitions: Seq[(InternalRow, Seq[Int])] = {
    // also sort the input partitions according to their partition key order. This ensures
    // a canonical order from both sides of a bucketed join, for example.

    val (projectedDataTypes, projectedKeys) =
      joinKeyPositions match {
        case Some(projectionPositions) =>
          val projectedDataTypes =
            projectExpressions(firstKeyedPartitioning.expressions).map(_.dataType)
          val projectedKeys = firstKeyedPartitioning.partitionKeys
            .map(KeyedPartitioning.projectKey(_, projectionPositions, projectedDataTypes))
          (projectedDataTypes, projectedKeys)

        case _ =>
          val dataTypes = firstKeyedPartitioning.expressions.map(_.dataType)
          (dataTypes, firstKeyedPartitioning.partitionKeys)
      }

    val reducedKeys = reducers match {
      case Some(reducers) =>
        projectedKeys.map(KeyedPartitioning.reduceKey(_, reducers, projectedDataTypes))
      case _ => projectedKeys
    }

    val internalRowComparableWrapperFactory =
      InternalRowComparableWrapper.getInternalRowComparableWrapperFactory(projectedDataTypes)

    val map = reducedKeys.zipWithIndex.groupMap {
      case (key, _) => internalRowComparableWrapperFactory(key)
    }(_._2)

    // When partially clustered, the input partitions are not grouped by partition
    // values. Here we'll need to check `commonPartitionValues` and decide how to group
    // and replicate splits within a partition.
    if (commonPartitionValues.isDefined) {
      commonPartitionValues.get.flatMap { case (key, numSplits) =>
        val splits = map.getOrElse(internalRowComparableWrapperFactory(key), Seq.empty)
        if (applyPartialClustering && !replicatePartitions) {
          splits.map(Seq(_)).padTo(numSplits, Seq.empty).map((key, _))
        } else {
          Seq.fill(numSplits)((key, splits))
        }
      }
    } else {
      val rowOrdering = RowOrdering.createNaturalAscendingOrdering(projectedDataTypes)
      map.toSeq
        .map { case (keyWrapper, v) => (keyWrapper.row, v) }
        .sorted(rowOrdering.on((t: (InternalRow, _)) => t._1))
    }
  }

  override protected def doExecute(): RDD[InternalRow] = {
    val partitionCoalescer = new GroupedPartitionCoalescer(groupedPartitions.map(_._2))
    if (groupedPartitions.isEmpty) {
      sparkContext.emptyRDD
    } else {
      new CoalescedRDD(child.execute(), groupedPartitions.size, Some(partitionCoalescer))
    }
  }

  override def output: Seq[Attribute] = child.output

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)

  override def outputOrdering: Seq[SortOrder] = {
    // when multiple partitions are grouped together, ordering inside partitions is not preserved
    if (groupedPartitions.forall(_._2.size <= 1)) {
      child.outputOrdering
    } else {
      super.outputOrdering
    }
  }
}

class GroupedPartitionCoalescer(
    val groupedPartitions: Seq[Seq[Int]]
  ) extends PartitionCoalescer with Serializable {

  override def coalesce(maxPartitions: Int, parent: RDD[_]): Array[PartitionGroup] = {
    groupedPartitions.map { partitionIndices =>
      val partitions = new ArrayBuffer[Partition](partitionIndices.size)
      val preferredLocations = new ArrayBuffer[String](partitionIndices.size)
      partitionIndices.foreach { partitionIndex =>
        val partition = parent.partitions(partitionIndex)
        partitions += partition
        preferredLocations ++= parent.preferredLocations(partition)
      }
      val preferredLocation =
        preferredLocations.groupBy(identity).transform((_, l) => l.size).maxByOption(_._2).map(_._1)
      val partitionGroup = new PartitionGroup(preferredLocation)
      partitionGroup.partitions ++= partitions
      partitionGroup
    }.toArray
  }
}
