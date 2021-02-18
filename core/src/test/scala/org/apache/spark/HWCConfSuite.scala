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

package org.apache.spark

import java.io.{File, FileOutputStream}
import java.nio.file.{Files, NoSuchFileException, Path}
import java.util.Properties

import org.mockito.Mockito.{doAnswer, spy}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.Matchers

import org.apache.spark.cloudera.HWCConf
import org.apache.spark.internal.config.{HWC_DEFAULTS_PATH, HWC_JAR}

class HWCConfSuite extends SparkFunSuite with Matchers {

  private val HWC_DIR = "hive_warehouse_connector"

  def withTempConfFile(fileName: String, confs: Map[String, String])(f: Path => Unit): Unit = {
    withTempDir { libDir =>
      val props = new Properties()
      confs.foreach { case (k, v) => props.put(k, v) }
      val hwcLibDir = new File(libDir, HWC_DIR)
      require(hwcLibDir.mkdir())
      val confFile = new File(hwcLibDir, fileName)
      val fos = new FileOutputStream(confFile)
      props.store(fos, "Temporary hwc-defaults.conf for testing")
      fos.close()

      f(confFile.toPath)
    }
  }

  def withHWCConfs(hwcConfs: Map[String, String])(f: HWCConf => Unit): Unit = {
    withTempConfFile("hwc-defaults.conf", hwcConfs) { filePath =>
      /* The HWCConf.hwcDefaultsConfPath methods looks for "SELF" in sys.env to generate the path
       * to "hwc-defaults.conf". Since we cannot modify sys.env from unittests, we need to mock the
       * hwcLibDir method to return path to a temp hwc-defaults.conf.
       */
      val hwcConf = HWCConf.get
      val spyHWCConf = spy(hwcConf)
      doAnswer(new Answer[Path] {
        override def answer(invocationOnMock: InvocationOnMock): Path = {
          val defaultSelf = filePath.getParent.resolveSibling("spark").resolve("conf")
          hwcConf.hwcLibDir(Some(defaultSelf.toString))
        }
      }).when(spyHWCConf).hwcLibDir(None)

      f(spyHWCConf)
    }
  }

  def sparkConfDir(libDir: File): String = libDir.toPath.resolve("spark").resolve("conf").toString

  test("hwcLibDir works") {
    withTempDir { libDir =>
      val selfPath = sparkConfDir(libDir)
      val hwcLibDir = new File(libDir, HWC_DIR)
      require(hwcLibDir.mkdirs())
      val expectedHWCLibDir = hwcLibDir.getAbsolutePath
      val actualHWCLibDir = HWCConf.get.hwcLibDir(Some(selfPath)).toString
      assert(actualHWCLibDir == expectedHWCLibDir)
    }
  }

  test("hwcLibDir fails when SELF is not found") {
    // While running unit tests SELF env variable is not set, in a CDH cluster spark-env.sh sets it.
    val caught = intercept[NoSuchFileException] {
      HWCConf.get.hwcLibDir()
    }
    assert(caught.getMessage == "Cannot find hive warehouse connector lib dir")
  }

  test("hwcLibDir fails when dir is not found") {
    val caught = intercept[IllegalArgumentException] {
      withTempDir { libDir =>
        val selfPath = sparkConfDir(libDir)
        HWCConf.get.hwcLibDir(Some(selfPath))
      }
    }
    assert(caught.getMessage.endsWith("not found"))
  }

  test("hwcLibDir fails when dir is not a directory") {
    val caught = intercept[IllegalArgumentException] {
      withTempDir { libDir =>
        Files.createFile(libDir.toPath.resolve(HWC_DIR))
        val selfPath = sparkConfDir(libDir)
        HWCConf.get.hwcLibDir(Some(selfPath))
      }
    }
    assert(caught.getMessage.endsWith("is not a directory"))
  }

  test("HWCConf.jarPath works") {
    (1 to 4).foreach { numberOfJars =>
      withHWCConfs(Map.empty) { hwcConf =>
        val hwcLibDirPath = hwcConf.hwcLibDir()
        val expectedJars = (1 to numberOfJars).map { i =>
          val jarPath = hwcLibDirPath.resolve(s"hwc-$i.jar")
          Files.createFile(jarPath)
          jarPath.toString
        }.toSet

        val actualJars = hwcConf.jars(new SparkConf(false)).split(",").toSet
        actualJars should equal(expectedJars)
      }
    }
  }

  test("failure when no jars are present in HWC lib dir") {
    withHWCConfs(Map.empty) { hwcConf =>
      val caught = intercept[NoSuchFileException] {
        hwcConf.jars(new SparkConf(false))
      }
      assert(caught.getMessage.startsWith("Cannot find any jars under"))
    }
  }

  test("specifying HWC dev jar in SparkConf takes precedence over jar in HWC lib dir") {
    withHWCConfs(Map.empty) { hwcConf =>
      val hwcLibDirPath = hwcConf.hwcLibDir()
      Files.createFile(hwcLibDirPath.resolve("hwc-jar-under-lib.jar"))

      val devJarPath = "/home/orion/hwc-dev.jar"
      val jar = hwcConf.jars(new SparkConf(false).set(HWC_JAR, devJarPath))
      jar shouldBe devJarPath
    }
  }

  test("no failure when dev jar is specified but no jar in HWC lib dir") {
    withHWCConfs(Map.empty) { hwcConf =>
      val devJarPath = "/home/orion/hwc-dev.jar"
      val jar = hwcConf.jars(new SparkConf(false).set(HWC_JAR, devJarPath))
      jar shouldBe devJarPath
    }
  }

  test("HWCConf.configs returns all values from hwc-defaults.conf") {
    val expectedConfs = Map("test.conf1" -> "val1", "test.conf2" -> "val2")
    withHWCConfs(expectedConfs) { hwcConf =>
      val actualConfs = hwcConf.configs(new SparkConf(false))
      actualConfs shouldBe expectedConfs
    }
  }

  test("dev conf file supersedes hwc-defaults.conf in HWC lib dir") {
    val defaultConfs = Map("test.conf1" -> "val1", "test.conf2" -> "val2")
    withHWCConfs(defaultConfs) { hwcConf =>
      val devConfs = Map("test.dev.conf1" -> "val1", "test.dev.conf2" -> "val2")
      withTempConfFile("hwc-dev.conf", devConfs) { devConfPath =>
        val sparkConf = new SparkConf(false).set(HWC_DEFAULTS_PATH, devConfPath.toString)
        val actualConfs = hwcConf.configs(sparkConf)
        actualConfs shouldBe devConfs
      }
    }
  }

  test("no failure when dev conf file is specified and no conf file in HWC lib dir") {
    val devConfs = Map("test.dev.conf1" -> "val1", "test.dev.conf2" -> "val2")
    withTempConfFile("hwc-dev.conf", devConfs) { devConfPath =>
      val sparkConf = new SparkConf(false).set(HWC_DEFAULTS_PATH, devConfPath.toString)
      val actualConfs = HWCConf.get.configs(sparkConf)
      actualConfs shouldBe devConfs
    }
  }

  test("configs in SparkConf takes precedence over configs from hwc-defaults.conf") {
    val hwcConfs = Map("test.conf1" -> "val1", "test.conf2" -> "val2")
    withHWCConfs(hwcConfs) { hwcConf =>
      val sparkConf = new SparkConf(false).set("test.conf1", "val3")
      val actualConfs = hwcConf.configs(sparkConf)

      val expectedConfs = Map("test.conf2" -> "val2")
      actualConfs shouldBe expectedConfs
    }
  }

  test("config merging works when config is present in hwc-defaults and SparkConf") {
    var hwcConfs = Map("spark.sql.extensions" -> "ext-from-hwc",
      "spark.kryo.registrator" -> "kryo-from-hwc")
    withHWCConfs(hwcConfs) { hwcConf =>
      val sparkConf = new SparkConf(false)
        .set("spark.sql.extensions", "ext-from-sparkConf")
        .set("spark.kryo.registrator", "kryo-from-sparkConf")
      val actualConfs = hwcConf.configs(sparkConf)

      val expectedConfs = Map("spark.sql.extensions" -> "ext-from-sparkConf,ext-from-hwc",
        "spark.kryo.registrator" -> "kryo-from-sparkConf,kryo-from-hwc")
      actualConfs shouldBe expectedConfs
    }

    hwcConfs = Map("spark.sql.extensions" -> "ext-from-hwc")
    withHWCConfs(hwcConfs) { hwcConf =>
      val sparkConf = new SparkConf(false).set("spark.kryo.registrator", "kryo-from-sparkConf")
      val actualConfs = hwcConf.configs(sparkConf)

      val expectedConfs = Map("spark.sql.extensions" -> "ext-from-hwc",
        "spark.kryo.registrator" -> "kryo-from-sparkConf")
      actualConfs shouldBe expectedConfs
    }
  }

  test("config merging works when config is present only in hwc-defaults") {
    val expectedConfs = Map("spark.sql.extensions" -> "ext-from-hwc",
      "spark.kryo.registrator" -> "kryo-from-hwc")
    withHWCConfs(expectedConfs) { hwcConf =>
      val actualConfs = hwcConf.configs(new SparkConf(false))
      actualConfs shouldBe expectedConfs
    }
  }

  test("config merging works when config is present only in SparkConf") {
    withHWCConfs(Map.empty) { hwcConf =>
      val sparkConf = new SparkConf(false)
        .set("spark.sql.extensions", "ext-from-sparkConf")
        .set("spark.kryo.registrator", "kryo-from-sparkConf")
      val actualConfs = hwcConf.configs(sparkConf)

      val expectedConfs = Map("spark.sql.extensions" -> "ext-from-sparkConf",
        "spark.kryo.registrator" -> "kryo-from-sparkConf")
      actualConfs shouldBe expectedConfs
    }
  }
}
