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

package org.apache.spark.sql.hive

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoTable, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources.{InsertIntoHadoopFsRelationCommand, LogicalRelation}
import org.apache.spark.sql.hive.execution._

/**
 * CDPD-454: Utility extractor for RunnableCommands
 */
private object IsRunnableCommand {
  def unapply(r: RunnableCommand): Option[TableIdentifier] = {
    r match {
      case LoadDataCommand(tableName, _, _, _, _) =>
        Some(tableName)

      case TruncateTableCommand(tableName, _) =>
        Some(tableName)

      case AlterTableChangeColumnCommand(tableName, _, _) =>
        Some(tableName)

      case AlterTableAddColumnsCommand(tableName, _) =>
        Some(tableName)

      case AlterTableSerDePropertiesCommand(tableName, _, _, _) =>
        Some(tableName)

      case AlterTableAddPartitionCommand(tableName, _, _) =>
        Some(tableName)

      case AlterTableRenamePartitionCommand(tableName, _, _) =>
        Some(tableName)

      case AlterTableDropPartitionCommand(tableName, _, _, _, _) =>
        Some(tableName)

      case AlterTableRecoverPartitionsCommand(tableName, _) =>
        Some(tableName)

      case AlterTableSetLocationCommand(tableName, _, _) =>
        Some(tableName)

      case AnalyzeTableCommand(tableName, _) =>
        Some(tableName)

      case AnalyzeColumnCommand(tableName, _) =>
        Some(tableName)

      case AnalyzePartitionCommand(tableName, _, _) =>
        Some(tableName)

      case _ => None
    }
  }
}

/**
 * CDPD-454: Checks if the operator will violate some restriction specified by
 * the HMS translation layer
 */
class HiveTranslationLayerCheck(session: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan resolveOperators {
      case i: InsertIntoHadoopFsRelationCommand =>
        i.catalogTable.foreach { tableMeta =>
          CatalogUtils.throwIfRO(tableMeta)
        }
        i

      case i @ InsertIntoTable(r: HiveTableRelation, _, _, _, _) =>
        CatalogUtils.throwIfRO(r.tableMeta)
        i

      case i @ InsertIntoHiveTable(tableMeta: CatalogTable, _, _, _, _, _) =>
        CatalogUtils.throwIfRO(tableMeta)
        i

      // The following 2 cases should not match if we have an earlier rule that swaps out the
      // relation node for a DataSourceV2Relation node that understands how to read
      // locked-down Hive objects. Therefore, if we have such a swapping rule, we should not end up
      // throwing an exception for a locked-down object
      case l @ LogicalRelation(_, _, Some(tableMeta), _) =>
        CatalogUtils.throwIfNoAccess(tableMeta)
        l

      case h @ HiveTableRelation(tableMeta, _, _) =>
        CatalogUtils.throwIfNoAccess(tableMeta)
        h

      case d @ DropTableCommand(tableName, _, _, _) =>
        CatalogUtils.throwIfDropNotAllowed(tableName, session.sessionState.catalog)
        d

      case r @ IsRunnableCommand(tableName) =>
        CatalogUtils.throwIfRO(tableName, session.sessionState.catalog)
        r
    }
  }
}

