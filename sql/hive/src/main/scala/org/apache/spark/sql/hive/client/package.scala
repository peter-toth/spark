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

/** Support for interacting with different versions of the HiveMetastoreClient */
package object client {
  private[hive] sealed abstract class HiveVersion(
      val fullVersion: String,
      val extraDeps: Seq[String] = Nil,
      val exclusions: Seq[String] = Nil) extends Ordered[HiveVersion] {
    override def compare(that: HiveVersion): Int = {
      // CDPD-19484: CDP hive version contains CDP version which should be cut.
      val thisVersionParts = fullVersion.split('.').slice(0, 3).map(_.toInt)
      val thatVersionParts = that.fullVersion.split('.').slice(0, 3).map(_.toInt)
      assert(thisVersionParts.length == thatVersionParts.length)
      thisVersionParts.zip(thatVersionParts).foreach { case (l, r) =>
        val candidate = l - r
        if (candidate != 0) {
          return candidate
        }
      }
      0
    }
  }

  // scalastyle:off
  private[hive] object hive {
    case object v12 extends HiveVersion("0.12.0")
    case object v13 extends HiveVersion("0.13.1")

    // Do not need Calcite because we disabled hive.cbo.enable.
    //
    // The other excluded dependencies are nowhere to be found, so exclude them explicitly. If
    // they're needed by the metastore client, users will have to dig them out of somewhere and use
    // configuration to point Spark at the correct jars.
    case object v14 extends HiveVersion("0.14.0",
      exclusions = Seq("org.apache.calcite:calcite-core",
        "org.apache.calcite:calcite-avatica",
        "org.pentaho:pentaho-aggdesigner-algorithm"))

    case object v1_0 extends HiveVersion("1.0.1",
      exclusions = Seq("eigenbase:eigenbase-properties",
        "org.apache.calcite:calcite-core",
        "org.apache.calcite:calcite-avatica",
        "org.pentaho:pentaho-aggdesigner-algorithm",
        "net.hydromatic:linq4j",
        "net.hydromatic:quidem"))

    // The curator dependency was added to the exclusions here because it seems to confuse the ivy
    // library. org.apache.curator:curator is a pom dependency but ivy tries to find the jar for it,
    // and fails.
    case object v1_1 extends HiveVersion("1.1.1",
      exclusions = Seq("eigenbase:eigenbase-properties",
        "org.apache.calcite:calcite-core",
        "org.apache.calcite:calcite-avatica",
        "org.apache.curator:*",
        "org.pentaho:pentaho-aggdesigner-algorithm",
        "net.hydromatic:linq4j",
        "net.hydromatic:quidem"))

    case object v1_2 extends HiveVersion("1.2.2",
      exclusions = Seq("eigenbase:eigenbase-properties",
        "org.apache.calcite:calcite-core",
        "org.apache.calcite:calcite-avatica",
        "org.apache.curator:*",
        "org.pentaho:pentaho-aggdesigner-algorithm",
        "net.hydromatic:linq4j",
        "net.hydromatic:quidem"))

    case object v2_0 extends HiveVersion("2.0.1",
      exclusions = Seq("org.apache.calcite:calcite-core",
        "org.apache.calcite:calcite-avatica",
        "org.apache.curator:*",
        "org.pentaho:pentaho-aggdesigner-algorithm"))

    case object v2_1 extends HiveVersion("2.1.1",
      exclusions = Seq("org.apache.calcite:calcite-core",
        "org.apache.calcite:calcite-avatica",
        "org.apache.curator:*",
        "org.pentaho:pentaho-aggdesigner-algorithm"))

    case object v2_2 extends HiveVersion("2.2.0",
      exclusions = Seq("org.apache.calcite:calcite-core",
        "org.apache.calcite:calcite-druid",
        "org.apache.calcite.avatica:avatica",
        "org.apache.curator:*",
        "org.pentaho:pentaho-aggdesigner-algorithm"))

    // Since HIVE-14496, Hive materialized view need calcite-core.
    // For spark, only VersionsSuite currently creates a hive materialized view for testing.
    case object v2_3 extends HiveVersion("2.3.7",
      exclusions = Seq("org.apache.calcite:calcite-druid",
        "org.apache.calcite.avatica:avatica",
        "org.apache.curator:*",
        "org.pentaho:pentaho-aggdesigner-algorithm"))

    // Since Hive 3.0, HookUtils uses org.apache.logging.log4j.util.Strings
    // Since HIVE-14496, Hive.java uses calcite-core
    case object v3_0 extends HiveVersion("3.0.0",
      extraDeps = Seq("org.apache.logging.log4j:log4j-api:2.10.0",
        "org.apache.derby:derby:10.14.1.0"),
      exclusions = Seq("org.apache.calcite:calcite-druid",
        "org.apache.calcite.avatica:avatica",
        "org.apache.curator:*",
        "org.pentaho:pentaho-aggdesigner-algorithm"))

    // Since Hive 3.0, HookUtils uses org.apache.logging.log4j.util.Strings
    // Since HIVE-14496, Hive.java uses calcite-core
    case object v3_1 extends HiveVersion("3.1.0",
      extraDeps = Seq("org.apache.logging.log4j:log4j-api:2.10.0",
        "org.apache.derby:derby:10.14.2.0"),
      exclusions = Seq("org.apache.calcite:calcite-druid",
        "org.apache.calcite.avatica:avatica",
        "org.apache.curator:*",
        "org.pentaho:pentaho-aggdesigner-algorithm"))

    case object vcdpd extends HiveVersion(HiveUtils.builtinHiveVersion,
      extraDeps = Seq("org.apache.logging.log4j:log4j-api:2.10.0",
        "org.apache.derby:derby:10.14.2.0"),
      exclusions = Seq("org.apache.calcite:calcite-druid",
        "org.apache.calcite.avatica:avatica",
        "org.apache.curator:*",
        "org.pentaho:pentaho-aggdesigner-algorithm",
        "com.google.guava:guava",
        "org.springframework:spring-framework-bom"))

    val allSupportedHiveVersions =
      Set(v12, v13, v14, v1_0, v1_1, v1_2, v2_0, v2_1, v2_2, v2_3, v3_0, v3_1, vcdpd)
  }
  // scalastyle:on

}
