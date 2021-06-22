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

package org.apache.spark.cloudera

import java.nio.file.{Files, NoSuchFileException, Path, Paths}

import scala.collection.JavaConverters._

import org.apache.commons.io.FilenameUtils
import org.apache.commons.lang3.StringUtils

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.{HWC_DEFAULTS_PATH, HWC_JAR}
import org.apache.spark.util.Utils

/**
 * Utility class to find the HWC jar(s) and load the HWC configs.
 */
private[spark] class HWCConf extends Logging {

  /**
   * Returns a comma separated string of all the HWC jars.
   *
   * If spark.cloudera.hwcJarPath is set in the SparkConf, then its value is returned.
   * Else all Jars in $PARCEL_ROOT/lib/hive_warehouse_connector dir are returned. A
   * [[NoSuchFileException]] is thrown if no jars are found under HWC lib dir.
   *
   * @param sparkConf Spark Conf
   * @return a comma-separated string of jar paths
   */
  def jars(sparkConf: SparkConf): String = sparkConf.get(HWC_JAR) match {
    case Some(hwcDevJar: String) => hwcDevJar
    case None =>
      val hwcLibDirPath: Path = hwcLibDir()
      // Converting java stream to scala to avoid confusion
      val hwcJars = Files.list(hwcLibDirPath).iterator().asScala
        .filter(path => FilenameUtils.getExtension(path.toString) == "jar")
      if (hwcJars.isEmpty) {
        throw new NoSuchFileException(s"Cannot find any jars under $hwcLibDirPath dir")
      }
      hwcJars.mkString(",")
  }

  /**
   * Returns the required config to enable HWC i.e.
   * spark.kryo.registrator=com.qubole.spark.hiveacid.util.HiveAcidKyroRegistrator
   * spark.sql.extensions=com.hortonworks.spark.sql.rule.Extensions
   *
   * If spark.cloudera.hwcDefaultsPath is set in SparkConf then only the configs present in the
   * corresponding file are included in the returned map.
   *
   * For a given config key, if the value exists in multiple places like spark-defaults.conf,
   * HWC_DEFAULTS_PATH and --conf, then the precedence order is as follows:
   * --conf > spark-defaults.conf > HWC_DEFAULTS_PATH
   *
   * @param sparkConf Spark Conf
   * @return a map of HWC configs
   */
  def configs(sparkConf: SparkConf): Map[String, String] = {
    val hwcDefaults = sparkConf.get(HWC_DEFAULTS_PATH) match {
      case Some(hwcConfPath) => Utils.getPropertiesFromFile(hwcConfPath)
      case None => Map("spark.sql.extensions" -> "com.hortonworks.spark.sql.rule.Extensions",
        "spark.kryo.registrator" -> "com.qubole.spark.hiveacid.util.HiveAcidKyroRegistrator")
    }

    val hwcConfs = scala.collection.mutable.HashMap[String, String]()
    /* In cases where the configs can have comma-separated values and are specified in both
    SparkConf and hwc-defaults.conf, we want to preserve both values and merge them.
     */
    Seq("spark.sql.extensions", "spark.kryo.registrator").foreach { key =>
      val mergedValue = Seq(sparkConf.get(key, ""), hwcDefaults.getOrElse(key, ""))
        .filter(StringUtils.isNotBlank)
        .mkString(",")

      if (StringUtils.isNotBlank(mergedValue)) {
        hwcConfs += key -> mergedValue
      }
    }

    /* This is to ensure hwc configs specified via --conf during spark-submit or spark-shell takes
    precedence over configs in HWC_DEFAULTS_PATH.
     */
    hwcConfs ++= hwcDefaults.filterNot { case (key, _) => sparkConf.contains(key) }
    hwcConfs.toMap
  }

  /**
   * This is made public for unit testing.
   *
   * @param defaultSelf exposed for testing
   * @return the HWC lib path
   */
  def hwcLibDir(defaultSelf: Option[String] = None): Path = {
    // SELF => "/opt/cloudera/parcels/CDH-<parcel-version>/lib/spark/conf"
    val hwcLibDir = (if (defaultSelf.isDefined) defaultSelf else sys.env.get("SELF"))
      .map(Paths.get(_))
      // goto spark dir
      .map(_.getParent)
      // get HWC path under lib
      .map(_.resolveSibling("hive_warehouse_connector"))
      .getOrElse(throw new NoSuchFileException("Cannot find hive warehouse connector lib dir"))

    require(Files.exists(hwcLibDir), s"$hwcLibDir not found")
    require(Files.isDirectory(hwcLibDir), s"$hwcLibDir is not a directory")
    hwcLibDir
  }
}

private[spark] object HWCConf {
  private val hwcConf = new HWCConf

  def get: HWCConf = hwcConf
}
