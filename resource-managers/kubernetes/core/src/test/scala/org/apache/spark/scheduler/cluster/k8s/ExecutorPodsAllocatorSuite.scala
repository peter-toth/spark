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
package org.apache.spark.scheduler.cluster.k8s

import java.time.Instant

import io.fabric8.kubernetes.api.model.{Pod, PodBuilder}
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.dsl.PodResource
import org.mockito.{ArgumentMatcher, Matchers, Mock, MockitoAnnotations}
import org.mockito.ArgumentMatchers.{any, eq => meq}
import org.mockito.Mockito.{never, timeout, times, verify, when}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.BeforeAndAfter

import org.apache.spark.{SecurityManager, SparkConf, SparkFunSuite}
import org.apache.spark.deploy.k8s.{KubernetesExecutorConf, KubernetesTestConf, SparkPod}
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.Fabric8Aliases._
import org.apache.spark.internal.config.{DYN_ALLOCATION_EXECUTOR_IDLE_TIMEOUT, DYN_ALLOCATION_INITIAL_EXECUTORS, DYN_ALLOCATION_MIN_EXECUTORS, EXECUTOR_INSTANCES}
import org.apache.spark.scheduler.cluster.k8s.ExecutorLifecycleTestUtils._
import org.apache.spark.util.ManualClock

class ExecutorPodsAllocatorSuite extends SparkFunSuite with BeforeAndAfter {

  private val driverPodName = "driver"

  private val driverPod = new PodBuilder()
    .withNewMetadata()
      .withName(driverPodName)
      .addToLabels(SPARK_APP_ID_LABEL, TEST_SPARK_APP_ID)
      .addToLabels(SPARK_ROLE_LABEL, SPARK_POD_DRIVER_ROLE)
      .withUid("driver-pod-uid")
      .endMetadata()
    .build()

  private val conf = new SparkConf()
    .set(KUBERNETES_DRIVER_POD_NAME, driverPodName)
    .set(DYN_ALLOCATION_EXECUTOR_IDLE_TIMEOUT.key, "10s")

  private val podAllocationSize = conf.get(KUBERNETES_ALLOCATION_BATCH_SIZE)
  private val podAllocationDelay = conf.get(KUBERNETES_ALLOCATION_BATCH_DELAY)
  private val executorIdleTimeout = conf.get(DYN_ALLOCATION_EXECUTOR_IDLE_TIMEOUT) * 1000
  private val podCreationTimeout = math.max(podAllocationDelay * 5,
    conf.get(KUBERNETES_ALLOCATION_EXECUTOR_TIMEOUT))

  private val secMgr = new SecurityManager(conf)

  private var waitForExecutorPodsClock: ManualClock = _

  @Mock
  private var kubernetesClient: KubernetesClient = _

  @Mock
  private var podOperations: PODS = _

  @Mock
  private var labeledPods: LABELED_PODS = _

  @Mock
  private var driverPodOperations: PodResource[Pod] = _

  @Mock
  private var executorBuilder: KubernetesExecutorBuilder = _

  private var snapshotsStore: DeterministicExecutorPodsSnapshotsStore = _

  private var podsAllocatorUnderTest: ExecutorPodsAllocator = _

  before {
    MockitoAnnotations.initMocks(this)
    when(kubernetesClient.pods()).thenReturn(podOperations)
    when(podOperations.withName(driverPodName)).thenReturn(driverPodOperations)
    when(driverPodOperations.get).thenReturn(driverPod)
    when(executorBuilder.buildFromFeatures(any(classOf[KubernetesExecutorConf]), meq(secMgr),
      meq(kubernetesClient))).thenAnswer(executorPodAnswer())
    snapshotsStore = new DeterministicExecutorPodsSnapshotsStore()
    waitForExecutorPodsClock = new ManualClock(0L)
    podsAllocatorUnderTest = new ExecutorPodsAllocator(
      conf, secMgr, executorBuilder, kubernetesClient, snapshotsStore, waitForExecutorPodsClock)
    podsAllocatorUnderTest.start(TEST_SPARK_APP_ID)
  }

  Seq(
    (EXECUTOR_INSTANCES.key, false),
    (DYN_ALLOCATION_INITIAL_EXECUTORS.key, true),
    (DYN_ALLOCATION_MIN_EXECUTORS.key, true)).foreach { case (targetCountConf, daEnabled) =>

    // create a test for each method of expressing the initial target executor count
    test("Use executor target for initial batch size when gang scheduling enabled: " +
      targetCountConf) {
      val batchSize = 5
      val initialExecutorCount = (batchSize * 2) + 1
      val conf = new SparkConf()
        .set(KUBERNETES_DRIVER_POD_NAME, driverPodName)
        .set(DYN_ALLOCATION_EXECUTOR_IDLE_TIMEOUT.key, "10s")
        .set(KUBERNETES_CLOUDERA_GANG_SCHEDULING.key, "true")
        .set(KUBERNETES_ALLOCATION_BATCH_SIZE, batchSize)
        .set("spark.dynamicAllocation.enabled", daEnabled.toString)
        .set(targetCountConf, initialExecutorCount.toString) // initial target count of executors

      // we create our own mock here because we need a different conf
      podsAllocatorUnderTest = new ExecutorPodsAllocator(
        conf, secMgr, executorBuilder, kubernetesClient, snapshotsStore, waitForExecutorPodsClock)
      podsAllocatorUnderTest.start(TEST_SPARK_APP_ID)

      // we set the expected executor target in two places: In the conf above (in
      // targetCountConf) and here. That's because ExecutorPodAllocator will use
      // the below value to determine the executor target, but the conf to determine the
      // batch size. In a real-world. non-mock environment, ExecutorAllocationManager
      // will first set totalExpectedExecutors based on the value returned from
      // Utils.getDynamicAllocationInitialExecutors, and ExecutorPodsAllocator
      // will call the same method to get the initial batch size. Since ExecutorAllocationManager
      // runs in its own thread and can keep updating totalExpectedExecutors, ExecutorPodsAllocator
      // always uses getDynamicAllocationInitialExecutors to determine the initial batch size.
      podsAllocatorUnderTest.setTotalExpectedExecutors(initialExecutorCount)

      // check that ExecutorPodsAllocator created initialExecutorCount pods,
      // not batchSize pods (contrast this with the next unit test)
      for (nextId <- 1 to initialExecutorCount) {
        verify(podOperations).create(podWithAttachedContainerForId(nextId))
      }

      // check that ExecutorPodsAllocator created no more than initialExecutorCount pods
      verify(podOperations, never()).create(podWithAttachedContainerForId(initialExecutorCount + 1))
    }
  }

  test("Initially request executors in batches. Do not request another batch if the" +
    " first has not finished.") {
    podsAllocatorUnderTest.setTotalExpectedExecutors(podAllocationSize + 1)
    for (nextId <- 1 to podAllocationSize) {
      verify(podOperations).create(podWithAttachedContainerForId(nextId))
    }
    verify(podOperations, never()).create(podWithAttachedContainerForId(podAllocationSize + 1))
  }

  test("Request executors in batches. Allow another batch to be requested if" +
    " all pending executors start running.") {
    podsAllocatorUnderTest.setTotalExpectedExecutors(podAllocationSize + 1)
    for (execId <- 1 until podAllocationSize) {
      snapshotsStore.updatePod(runningExecutor(execId))
    }
    snapshotsStore.notifySubscribers()
    verify(podOperations, never()).create(podWithAttachedContainerForId(podAllocationSize + 1))
    snapshotsStore.updatePod(runningExecutor(podAllocationSize))
    snapshotsStore.notifySubscribers()
    verify(podOperations).create(podWithAttachedContainerForId(podAllocationSize + 1))
    snapshotsStore.updatePod(runningExecutor(podAllocationSize))
    snapshotsStore.notifySubscribers()
    verify(podOperations, times(podAllocationSize + 1)).create(any(classOf[Pod]))
  }

  test("When a current batch reaches error states immediately, re-request" +
    " them on the next batch.") {
    podsAllocatorUnderTest.setTotalExpectedExecutors(podAllocationSize)
    for (execId <- 1 until podAllocationSize) {
      snapshotsStore.updatePod(runningExecutor(execId))
    }
    val failedPod = failedExecutorWithoutDeletion(podAllocationSize)
    snapshotsStore.updatePod(failedPod)
    snapshotsStore.notifySubscribers()
    verify(podOperations).create(podWithAttachedContainerForId(podAllocationSize + 1))
  }

  test("When an executor is requested but the API does not report it in a reasonable time, retry" +
    " requesting that executor.") {
    when(podOperations
      .withLabel(SPARK_APP_ID_LABEL, TEST_SPARK_APP_ID))
      .thenReturn(podOperations)
    when(podOperations
      .withLabel(SPARK_ROLE_LABEL, SPARK_POD_EXECUTOR_ROLE))
      .thenReturn(podOperations)
    when(podOperations
      .withLabelIn(SPARK_EXECUTOR_ID_LABEL, "1"))
      .thenReturn(labeledPods)
    podsAllocatorUnderTest.setTotalExpectedExecutors(1)
    verify(podOperations).create(podWithAttachedContainerForId(1))
    waitForExecutorPodsClock.setTime(podCreationTimeout + 1)
    snapshotsStore.notifySubscribers()
    verify(labeledPods).delete()
    verify(podOperations).create(podWithAttachedContainerForId(2))
  }

  test("SPARK-28487: scale up and down on target executor count changes") {
    when(podOperations
      .withField("status.phase", "Pending"))
      .thenReturn(podOperations)
    when(podOperations
      .withLabel(SPARK_APP_ID_LABEL, TEST_SPARK_APP_ID))
      .thenReturn(podOperations)
    when(podOperations
      .withLabel(SPARK_ROLE_LABEL, SPARK_POD_EXECUTOR_ROLE))
      .thenReturn(podOperations)
    when(podOperations
      .withLabelIn(meq(SPARK_EXECUTOR_ID_LABEL), any()))
      .thenReturn(podOperations)

    val startTime = Instant.now.toEpochMilli
    waitForExecutorPodsClock.setTime(startTime)

    // Target 1 executor, make sure it's requested, even with an empty initial snapshot.
    podsAllocatorUnderTest.setTotalExpectedExecutors(1)
    verify(podOperations).create(podWithAttachedContainerForId(1))

    // Mark executor as running, verify that subsequent allocation cycle is a no-op.
    snapshotsStore.updatePod(runningExecutor(1))
    snapshotsStore.notifySubscribers()
    verify(podOperations, times(1)).create(any())
    verify(podOperations, never()).delete()

    // Request 3 more executors, make sure all are requested.
    podsAllocatorUnderTest.setTotalExpectedExecutors(4)
    snapshotsStore.notifySubscribers()
    verify(podOperations).create(podWithAttachedContainerForId(2))
    verify(podOperations).create(podWithAttachedContainerForId(3))
    verify(podOperations).create(podWithAttachedContainerForId(4))

    // Mark 2 as running, 3 as pending. Allocation cycle should do nothing.
    snapshotsStore.updatePod(runningExecutor(2))
    snapshotsStore.updatePod(pendingExecutor(3))
    snapshotsStore.notifySubscribers()
    verify(podOperations, times(4)).create(any())
    verify(podOperations, never()).delete()

    // Scale down to 1. Pending executors (both acknowledged and not) should be deleted.
    waitForExecutorPodsClock.advance(executorIdleTimeout * 2)
    podsAllocatorUnderTest.setTotalExpectedExecutors(1)
    snapshotsStore.notifySubscribers()
    verify(podOperations, times(4)).create(any())
    verify(podOperations).withLabelIn(SPARK_EXECUTOR_ID_LABEL, "3", "4")
    verify(podOperations).delete()
    assert(podsAllocatorUnderTest.isDeleted("3"))
    assert(podsAllocatorUnderTest.isDeleted("4"))

    // Update the snapshot to not contain the deleted executors, make sure the
    // allocator cleans up internal state.
    snapshotsStore.updatePod(deletedExecutor(3))
    snapshotsStore.updatePod(deletedExecutor(4))
    snapshotsStore.removeDeletedExecutors()
    snapshotsStore.notifySubscribers()
    assert(!podsAllocatorUnderTest.isDeleted("3"))
    assert(!podsAllocatorUnderTest.isDeleted("4"))
  }

  test("SPARK-33099: Respect executor idle timeout configuration") {
    when(podOperations
      .withField("status.phase", "Pending"))
      .thenReturn(podOperations)
    when(podOperations
      .withLabel(SPARK_APP_ID_LABEL, TEST_SPARK_APP_ID))
      .thenReturn(podOperations)
    when(podOperations
      .withLabel(SPARK_ROLE_LABEL, SPARK_POD_EXECUTOR_ROLE))
      .thenReturn(podOperations)
    when(podOperations
      .withLabelIn(meq(SPARK_EXECUTOR_ID_LABEL), any()))
      .thenReturn(podOperations)

    val startTime = Instant.now.toEpochMilli
    waitForExecutorPodsClock.setTime(startTime)

    podsAllocatorUnderTest.setTotalExpectedExecutors(5)
    verify(podOperations).create(podWithAttachedContainerForId(1))
    verify(podOperations).create(podWithAttachedContainerForId(2))
    verify(podOperations).create(podWithAttachedContainerForId(3))
    verify(podOperations).create(podWithAttachedContainerForId(4))
    verify(podOperations).create(podWithAttachedContainerForId(5))
    verify(podOperations, times(5)).create(any())

    snapshotsStore.updatePod(pendingExecutor(1))
    snapshotsStore.updatePod(pendingExecutor(2))

    // Newly created executors (both acknowledged and not) are protected by executorIdleTimeout
    podsAllocatorUnderTest.setTotalExpectedExecutors(0)
    snapshotsStore.notifySubscribers()
    verify(podOperations, never()).withLabelIn(SPARK_EXECUTOR_ID_LABEL, "1", "2", "3", "4", "5")
    verify(podOperations, never()).delete()

    // Newly created executors (both acknowledged and not) are cleaned up.
    waitForExecutorPodsClock.advance(executorIdleTimeout * 2)
    snapshotsStore.notifySubscribers()
    verify(podOperations).withLabelIn(SPARK_EXECUTOR_ID_LABEL, "1", "2", "3", "4", "5")
    verify(podOperations).delete()
  }

  test("SPARK-33262: pod allocator does not stall with pending pods") {
    when(podOperations
      .withLabel(SPARK_APP_ID_LABEL, TEST_SPARK_APP_ID))
      .thenReturn(podOperations)
    when(podOperations
      .withLabel(SPARK_ROLE_LABEL, SPARK_POD_EXECUTOR_ROLE))
      .thenReturn(podOperations)
    when(podOperations
      .withLabelIn(SPARK_EXECUTOR_ID_LABEL, "1"))
      .thenReturn(labeledPods)
    when(podOperations
      .withLabelIn(SPARK_EXECUTOR_ID_LABEL, "2", "3", "4", "5", "6"))
      .thenReturn(podOperations)

    podsAllocatorUnderTest.setTotalExpectedExecutors(6)
    // Initial request of pods
    verify(podOperations).create(podWithAttachedContainerForId(1))
    verify(podOperations).create(podWithAttachedContainerForId(2))
    verify(podOperations).create(podWithAttachedContainerForId(3))
    verify(podOperations).create(podWithAttachedContainerForId(4))
    verify(podOperations).create(podWithAttachedContainerForId(5))
    // 4 come up, 1 pending
    snapshotsStore.updatePod(pendingExecutor(1))
    snapshotsStore.updatePod(runningExecutor(2))
    snapshotsStore.updatePod(runningExecutor(3))
    snapshotsStore.updatePod(runningExecutor(4))
    snapshotsStore.updatePod(runningExecutor(5))
    // We move forward one allocation cycle
    waitForExecutorPodsClock.setTime(podAllocationDelay + 1)
    snapshotsStore.notifySubscribers()
    // We request pod 6
    verify(podOperations).create(podWithAttachedContainerForId(6))
  }

  private def executorPodAnswer(): Answer[SparkPod] = {
    new Answer[SparkPod] {
      override def answer(invocation: InvocationOnMock): SparkPod = {
        val k8sConf: KubernetesExecutorConf = invocation.getArgument(0)
        executorPodWithId(k8sConf.executorId.toInt)
      }
    }
  }
}
