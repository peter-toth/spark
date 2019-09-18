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

package org.apache.spark.sql.hive.thriftserver

import java.sql.{DatabaseMetaData, ResultSet}

class SparkMetadataOperationSuite extends HiveThriftJdbcTest {

  override def mode: ServerMode.Value = ServerMode.binary

  test("Spark's own GetSchemasOperation(SparkGetSchemasOperation)") {
    def checkResult(rs: ResultSet, dbNames: Seq[String]): Unit = {
      for (i <- dbNames.indices) {
        assert(rs.next())
        assert(rs.getString("TABLE_SCHEM") === dbNames(i))
      }
      // Make sure there are no more elements
      assert(!rs.next())
    }

    withDatabase("db1", "db2") { statement =>
      Seq("CREATE DATABASE db1", "CREATE DATABASE db2").foreach(statement.execute)

      val metaData = statement.getConnection.getMetaData

      checkResult(metaData.getSchemas(null, "%"), Seq("db1", "db2", "default", "global_temp"))
      checkResult(metaData.getSchemas(null, "db1"), Seq("db1"))
      checkResult(metaData.getSchemas(null, "db_not_exist"), Seq.empty)
      checkResult(metaData.getSchemas(null, "db*"), Seq("db1", "db2"))
    }
  }

  test("Spark's own GetTablesOperation(SparkGetTablesOperation)") {
    def checkResult(rs: ResultSet, tableNames: Seq[String]): Unit = {
      for (i <- tableNames.indices) {
        assert(rs.next())
        assert(rs.getString("TABLE_NAME") === tableNames(i))
      }
      // Make sure there are no more elements
      assert(!rs.next())
    }

    withJdbcStatement("table1", "table2", "view1") { statement =>
      Seq(
        "CREATE TABLE table1(key INT, val STRING)",
        "CREATE TABLE table2(key INT, val STRING)",
        "CREATE VIEW view1 AS SELECT * FROM table2",
        "CREATE OR REPLACE GLOBAL TEMPORARY VIEW view_global_temp_1 AS SELECT 1 AS col1",
        "CREATE OR REPLACE TEMPORARY VIEW view_temp_1 AS SELECT 1 as col1"
      ).foreach(statement.execute)

      val metaData = statement.getConnection.getMetaData

      checkResult(metaData.getTables(null, "%", "%", null),
        Seq("table1", "table2", "view1", "view_global_temp_1", "view_temp_1"))

      checkResult(metaData.getTables(null, "%", "table1", null), Seq("table1"))

      checkResult(metaData.getTables(null, "%", "table_not_exist", null), Seq.empty)

      checkResult(metaData.getTables(null, "%", "%", Array("TABLE")),
        Seq("table1", "table2"))

      checkResult(metaData.getTables(null, "%", "%", Array("VIEW")),
        Seq("view1", "view_global_temp_1", "view_temp_1"))

      checkResult(metaData.getTables(null, "%", "view_global_temp_1", null),
        Seq("view_global_temp_1"))

      checkResult(metaData.getTables(null, "%", "view_temp_1", null),
        Seq("view_temp_1"))

      checkResult(metaData.getTables(null, "%", "%", Array("TABLE", "VIEW")),
        Seq("table1", "table2", "view1", "view_global_temp_1", "view_temp_1"))

      checkResult(metaData.getTables(null, "%", "table_not_exist", Array("TABLE", "VIEW")),
        Seq.empty)
    }
  }

  test("Spark's own GetColumnsOperation(SparkGetColumnsOperation)") {
    def checkResult(
        rs: ResultSet,
        columns: Seq[(String, String, String, String, String)]) : Unit = {
      for (i <- columns.indices) {
        assert(rs.next())
        val col = columns(i)
        assert(rs.getString("TABLE_NAME") === col._1)
        assert(rs.getString("COLUMN_NAME") === col._2)
        assert(rs.getString("DATA_TYPE") === col._3)
        assert(rs.getString("TYPE_NAME") === col._4)
        assert(rs.getString("REMARKS") === col._5)
      }
      // Make sure there are no more elements
      assert(!rs.next())
    }

    withJdbcStatement("table1", "table2", "view1") { statement =>
      Seq(
        "CREATE TABLE table1(key INT comment 'Int column', val STRING comment 'String column')",
        "CREATE TABLE table2(key INT, val DECIMAL comment 'Decimal column')",
        "CREATE VIEW view1 AS SELECT key FROM table1",
        "CREATE OR REPLACE GLOBAL TEMPORARY VIEW view_global_temp_1 AS SELECT 2 AS col2",
        "CREATE OR REPLACE TEMPORARY VIEW view_temp_1 AS SELECT 2 as col2"
      ).foreach(statement.execute)

      val metaData = statement.getConnection.getMetaData

      checkResult(metaData.getColumns(null, "%", "%", null),
        Seq(
          ("table1", "key", "4", "INT", "Int column"),
          ("table1", "val", "12", "STRING", "String column"),
          ("table2", "key", "4", "INT", ""),
          ("table2", "val", "3", "DECIMAL(10,0)", "Decimal column"),
          ("view1", "key", "4", "INT", "Int column"),
          ("view_global_temp_1", "col2", "4", "INT", ""),
          ("view_temp_1", "col2", "4", "INT", "")))

      checkResult(metaData.getColumns(null, "%", "table1", null),
        Seq(
          ("table1", "key", "4", "INT", "Int column"),
          ("table1", "val", "12", "STRING", "String column")))

      checkResult(metaData.getColumns(null, "%", "table1", "key"),
        Seq(("table1", "key", "4", "INT", "Int column")))

      checkResult(metaData.getColumns(null, "%", "view%", null),
        Seq(
          ("view1", "key", "4", "INT", "Int column"),
          ("view_global_temp_1", "col2", "4", "INT", ""),
          ("view_temp_1", "col2", "4", "INT", "")))

      checkResult(metaData.getColumns(null, "%", "view_global_temp_1", null),
        Seq(("view_global_temp_1", "col2", "4", "INT", "")))

      checkResult(metaData.getColumns(null, "%", "view_temp_1", null),
        Seq(("view_temp_1", "col2", "4", "INT", "")))

      checkResult(metaData.getColumns(null, "%", "view_temp_1", "col2"),
        Seq(("view_temp_1", "col2", "4", "INT", "")))

      checkResult(metaData.getColumns(null, "default", "%", null),
        Seq(
          ("table1", "key", "4", "INT", "Int column"),
          ("table1", "val", "12", "STRING", "String column"),
          ("table2", "key", "4", "INT", ""),
          ("table2", "val", "3", "DECIMAL(10,0)", "Decimal column"),
          ("view1", "key", "4", "INT", "Int column"),
          ("view_temp_1", "col2", "4", "INT", "")))

      checkResult(metaData.getColumns(null, "%", "table_not_exist", null), Seq.empty)
    }
  }

  test("Spark's own GetTableTypesOperation(SparkGetTableTypesOperation)") {
    def checkResult(rs: ResultSet, tableTypes: Seq[String]): Unit = {
      for (i <- tableTypes.indices) {
        assert(rs.next())
        assert(rs.getString("TABLE_TYPE") === tableTypes(i))
      }
      // Make sure there are no more elements
      assert(!rs.next())
    }

    withJdbcStatement() { statement =>
      val metaData = statement.getConnection.getMetaData
      checkResult(metaData.getTableTypes, Seq("TABLE", "VIEW"))
    }
  }

  // The following code is commented since it was introducing compile failures
  // All of the tests in below code exist in other parts of this file with the same name.
  // The code was brought during backport of several commits from upstream and
  // to avoid confusion I am commenting them out, instead of removing them.
  /*
   test("Spark's own GetTablesOperation(SparkGetTablesOperation)") {
    def testGetTablesOperation(
        schema: String,
        tableNamePattern: String,
        tableTypes: JList[String])(f: HiveQueryResultSet => Unit): Unit = {
      val rawTransport = new TSocket("localhost", serverPort)
      val connection = new HiveConnection(s"jdbc:hive2://localhost:$serverPort", new Properties)
      val user = System.getProperty("user.name")
      val transport = PlainSaslHelper.getPlainTransport(user, "anonymous", rawTransport)
      val client = new TCLIService.Client(new TBinaryProtocol(transport))
      transport.open()

      var rs: HiveQueryResultSet = null

      try {
        val openResp = client.OpenSession(new TOpenSessionReq)
        val sessHandle = openResp.getSessionHandle

        val getTableReq = new TGetTablesReq(sessHandle)
        getTableReq.setSchemaName(schema)
        getTableReq.setTableName(tableNamePattern)
        getTableReq.setTableTypes(tableTypes)

        val getTableResp = client.GetTables(getTableReq)

        JdbcUtils.verifySuccess(getTableResp.getStatus)

        rs = new HiveQueryResultSet.Builder(connection)
          .setClient(client)
          .setSessionHandle(sessHandle)
          .setStmtHandle(getTableResp.getOperationHandle)
          .build()

        f(rs)
      } finally {
        rs.close()
        connection.close()
        transport.close()
        rawTransport.close()
      }
    }

    def checkResult(tableNames: Seq[String], rs: HiveQueryResultSet): Unit = {
      if (tableNames.nonEmpty) {
        for (i <- tableNames.indices) {
          assert(rs.next())
          assert(rs.getString("TABLE_NAME") === tableNames(i))
        }
      } else {
        assert(!rs.next())
      }
    }

    withJdbcStatement("table1", "table2") { statement =>
      Seq(
        "CREATE TABLE table1(key INT, val STRING)",
        "CREATE TABLE table2(key INT, val STRING)",
        "CREATE VIEW view1 AS SELECT * FROM table2").foreach(statement.execute)

      testGetTablesOperation("%", "%", null) { rs =>
        checkResult(Seq("table1", "table2", "view1"), rs)
      }

      testGetTablesOperation("%", "table1", null) { rs =>
        checkResult(Seq("table1"), rs)
      }

      testGetTablesOperation("%", "table_not_exist", null) { rs =>
        checkResult(Seq.empty, rs)
      }

      testGetTablesOperation("%", "%", JArrays.asList("TABLE")) { rs =>
        checkResult(Seq("table1", "table2"), rs)
      }

      testGetTablesOperation("%", "%", JArrays.asList("VIEW")) { rs =>
        checkResult(Seq("view1"), rs)
      }

      testGetTablesOperation("%", "%", JArrays.asList("TABLE", "VIEW")) { rs =>
        checkResult(Seq("table1", "table2", "view1"), rs)
      }
    }
  }

  test("Spark's own GetColumnsOperation(SparkGetColumnsOperation)") {
    def testGetColumnsOperation(
        schema: String,
        tableNamePattern: String,
        columnNamePattern: String)(f: HiveQueryResultSet => Unit): Unit = {
      val rawTransport = new TSocket("localhost", serverPort)
      val connection = new HiveConnection(s"jdbc:hive2://localhost:$serverPort", new Properties)
      val user = System.getProperty("user.name")
      val transport = PlainSaslHelper.getPlainTransport(user, "anonymous", rawTransport)
      val client = new ThriftserverShimUtils.Client(new TBinaryProtocol(transport))
      transport.open()

      var rs: HiveQueryResultSet = null

      try {
        val openResp = client.OpenSession(new ThriftserverShimUtils.TOpenSessionReq)
        val sessHandle = openResp.getSessionHandle

        val getColumnsReq = new ThriftserverShimUtils.TGetColumnsReq(sessHandle)
        getColumnsReq.setSchemaName(schema)
        getColumnsReq.setTableName(tableNamePattern)
        getColumnsReq.setColumnName(columnNamePattern)

        rs = new HiveQueryResultSet.Builder(connection)
          .setClient(client)
          .setSessionHandle(sessHandle)
          .setStmtHandle(client.GetColumns(getColumnsReq).getOperationHandle)
          .build()

        f(rs)
      } finally {
        rs.close()
        connection.close()
        transport.close()
        rawTransport.close()
      }
    }

    def checkResult(
        columns: Seq[(String, String, String, String, String)],
        rs: HiveQueryResultSet) : Unit = {
      if (columns.nonEmpty) {
        for (i <- columns.indices) {
          assert(rs.next())
          val col = columns(i)
          assert(rs.getString("TABLE_NAME") === col._1)
          assert(rs.getString("COLUMN_NAME") === col._2)
          assert(rs.getString("DATA_TYPE") === col._3)
          assert(rs.getString("TYPE_NAME") === col._4)
          assert(rs.getString("REMARKS") === col._5)
        }
      } else {
        assert(!rs.next())
      }
    }

    withJdbcStatement("table1", "table2", "view1") { statement =>
      Seq(
        "CREATE TABLE table1(key INT comment 'Int column', val STRING comment 'String column')",
        "CREATE TABLE table2(key INT, val DECIMAL comment 'Decimal column')",
        "CREATE VIEW view1 AS SELECT key FROM table1"
      ).foreach(statement.execute)

      testGetColumnsOperation("%", "%", null) { rs =>
        checkResult(
          Seq(
            ("table1", "key", "4", "INT", "Int column"),
            ("table1", "val", "12", "STRING", "String column"),
            ("table2", "key", "4", "INT", ""),
            ("table2", "val", "3", "DECIMAL(10,0)", "Decimal column"),
            ("view1", "key", "4", "INT", "Int column")), rs)
      }

      testGetColumnsOperation("%", "table1", null) { rs =>
        checkResult(
          Seq(
            ("table1", "key", "4", "INT", "Int column"),
            ("table1", "val", "12", "STRING", "String column")), rs)
      }

      testGetColumnsOperation("%", "table1", "key") { rs =>
        checkResult(Seq(("table1", "key", "4", "INT", "Int column")), rs)
      }

      testGetColumnsOperation("%", "table_not_exist", null) { rs =>
        checkResult(Seq.empty, rs)
      }
    }
  }

  test("GetTypeInfo Thrift API") {
    def checkResult(rs: ResultSet, typeNames: Seq[String]): Unit = {
      for (i <- typeNames.indices) {
        assert(rs.next())
        assert(rs.getString("TYPE_NAME") === typeNames(i))
      }
      // Make sure there are no more elements
      assert(!rs.next())
    }

    withJdbcStatement() { statement =>
      val metaData = statement.getConnection.getMetaData
      checkResult(metaData.getTypeInfo, ThriftserverShimUtils.supportedType().map(_.getName))
    }
  }

  test("Spark's own GetTableTypesOperation(SparkGetTableTypesOperation)") {
    def checkResult(rs: ResultSet, tableTypes: Seq[String]): Unit = {
      for (i <- tableTypes.indices) {
        assert(rs.next())
        assert(rs.getString("TABLE_TYPE") === tableTypes(i))
      }
      // Make sure there are no more elements
      assert(!rs.next())
    }

    withJdbcStatement() { statement =>
      val metaData = statement.getConnection.getMetaData
      checkResult(metaData.getTableTypes, Seq("TABLE", "VIEW"))
    }
  }
   */

  test("GetTypeInfo Thrift API") {
    def checkResult(rs: ResultSet, typeNames: Seq[String]): Unit = {
      for (i <- typeNames.indices) {
        assert(rs.next())
        assert(rs.getString("TYPE_NAME") === typeNames(i))
      }
      // Make sure there are no more elements
      assert(!rs.next())
    }

    withJdbcStatement() { statement =>
      val metaData = statement.getConnection.getMetaData
      checkResult(metaData.getTypeInfo, ThriftserverShimUtils.supportedType().map(_.getName))
    }
  }
}
