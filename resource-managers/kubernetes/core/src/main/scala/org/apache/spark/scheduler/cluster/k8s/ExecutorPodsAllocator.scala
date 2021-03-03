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
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import io.fabric8.kubernetes.api.model.PodBuilder
import io.fabric8.kubernetes.client.KubernetesClient
import scala.collection.mutable

import org.apache.spark.{SecurityManager, SparkConf, SparkException}
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.KubernetesConf
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.DYN_ALLOCATION_EXECUTOR_IDLE_TIMEOUT
import org.apache.spark.util.{Clock, Utils}

private[spark] class ExecutorPodsAllocator(
    conf: SparkConf,
    secMgr: SecurityManager,
    executorBuilder: KubernetesExecutorBuilder,
    kubernetesClient: KubernetesClient,
    snapshotsStore: ExecutorPodsSnapshotsStore,
    clock: Clock) extends Logging {

  private val EXECUTOR_ID_COUNTER = new AtomicLong(0L)

  private val totalExpectedExecutors = new AtomicInteger(0)

  private val podAllocationSize = conf.get(KUBERNETES_ALLOCATION_BATCH_SIZE)

  private val initialPodAllocationSize = if (conf.get(KUBERNETES_CLOUDERA_GANG_SCHEDULING)) {
    math.max(Utils.getDynamicAllocationInitialExecutors(conf), podAllocationSize)
  } else {
    podAllocationSize
  }

  private val podAllocationDelay = conf.get(KUBERNETES_ALLOCATION_BATCH_DELAY)

  private val podCreationTimeout = math.max(
    podAllocationDelay * 5,
    conf.get(KUBERNETES_ALLOCATION_EXECUTOR_TIMEOUT))

  private val executorIdleTimeout = conf.get(DYN_ALLOCATION_EXECUTOR_IDLE_TIMEOUT) * 1000

  private val namespace = conf.get(KUBERNETES_NAMESPACE)

  private val kubernetesDriverPodName = conf
    .get(KUBERNETES_DRIVER_POD_NAME)

  private val shouldDeleteExecutors = conf.get(KUBERNETES_DELETE_EXECUTORS)

  private val driverPod = kubernetesDriverPodName
    .map(name => Option(kubernetesClient.pods()
      .withName(name)
      .get())
      .getOrElse(throw new SparkException(
        s"No pod was found named $kubernetesDriverPodName in the cluster in the " +
          s"namespace $namespace (this was supposed to be the driver pod.).")))

  // Executor IDs that have been requested from Kubernetes but have not been detected in any
  // snapshot yet. Mapped to the timestamp when they were created.
  private val newlyCreatedExecutors = mutable.LinkedHashMap.empty[Long, Long]

  // Executor IDs that have been requested from Kubernetes but have not been detected in any POD
  // snapshot yet but already known by the scheduler backend.
  private val schedulerKnownNewlyCreatedExecs = mutable.LinkedHashSet.empty[Long]

  private val dynamicAllocationEnabled = Utils.isDynamicAllocationEnabled(conf)

  // visible for tests
  private[k8s] val numOutstandingPods = new AtomicInteger()

  private var lastSnapshot = ExecutorPodsSnapshot()

  private var isFirstRound = true

  // Executors that have been deleted by this allocator but not yet detected as deleted in
  // a snapshot from the API server. This is used to deny registration from these executors
  // if they happen to come up before the deletion takes effect.
  @volatile private var deletedExecutorIds = Set.empty[Long]

  def start(applicationId: String, schedulerBackend: KubernetesClusterSchedulerBackend): Unit = {
    snapshotsStore.addSubscriber(podAllocationDelay) {
      onNewSnapshots(applicationId, schedulerBackend, _)
    }
  }

  def setTotalExpectedExecutors(total: Int): Unit = {
    logDebug(s"Set totalExpectedExecutors to $total")
    totalExpectedExecutors.set(total)
    if (numOutstandingPods.get() == 0) {
      snapshotsStore.notifySubscribers()
    }
  }

  def isDeleted(executorId: String): Boolean = deletedExecutorIds.contains(executorId.toLong)

  private def onNewSnapshots(
      applicationId: String,
      schedulerBackend: KubernetesClusterSchedulerBackend,
      snapshots: Seq[ExecutorPodsSnapshot]): Unit = {
    val k8sKnownExecIds = snapshots.flatMap(_.executorPods.keys)
    newlyCreatedExecutors --= k8sKnownExecIds
    schedulerKnownNewlyCreatedExecs --= k8sKnownExecIds

    // transfer the scheduler backend known executor requests from the newlyCreatedExecutors
    // to the schedulerKnownNewlyCreatedExecs
    val schedulerKnownExecs = schedulerBackend.getExecutorIds().map(_.toLong).toSet
    schedulerKnownNewlyCreatedExecs ++=
      newlyCreatedExecutors.filterKeys(schedulerKnownExecs.contains(_)).keySet
    newlyCreatedExecutors --= schedulerKnownNewlyCreatedExecs

    // For all executors we've created against the API but have not seen in a snapshot
    // yet - check the current time. If the current time has exceeded some threshold,
    // assume that the pod was either never created (the API server never properly
    // handled the creation request), or the API server created the pod but we missed
    // both the creation and deletion events. In either case, delete the missing pod
    // if possible, and mark such a pod to be rescheduled below.
    val currentTime = clock.getTimeMillis()
    val timedOut = newlyCreatedExecutors.flatMap { case (execId, timeCreated) =>
      if (currentTime - timeCreated > podCreationTimeout) {
        Some(execId)
      } else {
        logDebug(s"Executor with id $execId was not found in the Kubernetes cluster since it" +
          s" was created ${currentTime - timeCreated} milliseconds ago.")
        None
      }
    }

    if (timedOut.nonEmpty) {
      logWarning(s"Executors with ids ${timedOut.mkString(",")} were not detected in the" +
        s" Kubernetes cluster after $podCreationTimeout ms despite the fact that a previous" +
        " allocation attempt tried to create them. The executors may have been deleted but the" +
        " application missed the deletion event.")

      newlyCreatedExecutors --= timedOut
      if (shouldDeleteExecutors) {
        Utils.tryLogNonFatalError {
          kubernetesClient
            .pods()
            .withLabel(SPARK_APP_ID_LABEL, applicationId)
            .withLabel(SPARK_ROLE_LABEL, SPARK_POD_EXECUTOR_ROLE)
            .withLabelIn(SPARK_EXECUTOR_ID_LABEL, timedOut.toSeq.map(_.toString): _*)
            .delete()
        }
      }
    }

    if (snapshots.nonEmpty) {
      lastSnapshot = snapshots.last
    }

    // Make a local, non-volatile copy of the reference since it's used multiple times. This
    // is the only method that modifies the list, so this is safe.
    var _deletedExecutorIds = deletedExecutorIds

    val executorPods = lastSnapshot.executorPods.filterKeys(!_deletedExecutorIds.contains(_))

    val currentRunningCount = executorPods.values.count {
      case PodRunning(_) => true
      case _ => false
    }

    val (schedulerKnownPendingExecs, currentPendingExecutors) = executorPods
      .filter {
        case (_, PodPending(_)) => true
        case _ => false
      }.partition { case (k, _) =>
        schedulerKnownExecs.contains(k)
      }

    if (snapshots.nonEmpty) {
      logDebug(s"Pod allocation status: $currentRunningCount running, " +
         s"${currentPendingExecutors.size} unknown pending, " +
         s"${schedulerKnownPendingExecs.size} scheduler backend known pending, " +
         s"${newlyCreatedExecutors.size} unknown newly created, " +
         s"${schedulerKnownNewlyCreatedExecs.size} scheduler backend known newly created.")

      val existingExecs = lastSnapshot.executorPods.keySet
      _deletedExecutorIds = _deletedExecutorIds.filter(existingExecs.contains)
    }

    val currentTotalExpectedExecutors = totalExpectedExecutors.get

    // This variable is used later to print some debug logs. It's updated when cleaning up
    // excess pod requests, since currentPendingExecutors is immutable.
    var knownPendingCount = currentPendingExecutors.size

    // It's possible that we have outstanding pods that are outdated when dynamic allocation
    // decides to downscale the application. So check if we can release any pending pods early
    // instead of waiting for them to time out. Drop them first from the unacknowledged list,
    // then from the pending. However, in order to prevent too frequent frunctuation, newly
    // requested pods are protected during executorIdleTimeout period.
    //
    // TODO: with dynamic allocation off, handle edge cases if we end up with more running
    // executors than expected.
    val knownPodCount = currentRunningCount +
       currentPendingExecutors.size + schedulerKnownPendingExecs.size +
       newlyCreatedExecutors.size + schedulerKnownNewlyCreatedExecs.size
    if (knownPodCount > currentTotalExpectedExecutors) {
      val excess = knownPodCount - currentTotalExpectedExecutors
      val newlyCreatedToDelete = newlyCreatedExecutors
        .filter(x => currentTime - x._2 > executorIdleTimeout)
        .keys.take(excess).toList
      val knownPendingToDelete = currentPendingExecutors
        .filter(x => isExecutorIdleTimedOut(x._2, currentTime))
        .take(excess - newlyCreatedToDelete.size)
        .map { case (id, _) => id }
      val toDelete = newlyCreatedToDelete ++ knownPendingToDelete

      if (toDelete.nonEmpty) {
        logInfo(s"Deleting ${toDelete.size} excess pod requests (${toDelete.mkString(",")}).")
        _deletedExecutorIds = _deletedExecutorIds ++ toDelete

        Utils.tryLogNonFatalError {
          kubernetesClient
            .pods()
            .withField("status.phase", "Pending")
            .withLabel(SPARK_APP_ID_LABEL, applicationId)
            .withLabel(SPARK_ROLE_LABEL, SPARK_POD_EXECUTOR_ROLE)
            .withLabelIn(SPARK_EXECUTOR_ID_LABEL, toDelete.sorted.map(_.toString): _*)
            .delete()
          newlyCreatedExecutors --= newlyCreatedToDelete
          knownPendingCount -= knownPendingToDelete.size
        }
      }
    }

    if (newlyCreatedExecutors.isEmpty
        && knownPodCount < currentTotalExpectedExecutors) {

      val _podAllocationSize = if (isFirstRound) {
        isFirstRound = false
        logInfo(s"First round of allocations: use batch size $initialPodAllocationSize")
        initialPodAllocationSize
      } else {
        logDebug(s"Subsequent round of allocations: use batch size $podAllocationSize")
        podAllocationSize
      }
      val numExecutorsToAllocate = math.min(
        currentTotalExpectedExecutors - knownPodCount, _podAllocationSize)
      logInfo(s"Going to request $numExecutorsToAllocate executors from Kubernetes.")
      for ( _ <- 0 until numExecutorsToAllocate) {
        val newExecutorId = EXECUTOR_ID_COUNTER.incrementAndGet()
        val executorConf = KubernetesConf.createExecutorConf(
          conf,
          newExecutorId.toString,
          applicationId,
          driverPod)
        val executorPod = executorBuilder.buildFromFeatures(executorConf, secMgr,
          kubernetesClient)
        val podWithAttachedContainer = new PodBuilder(executorPod.pod)
          .editOrNewSpec()
          .addToContainers(executorPod.container)
          .endSpec()
          .build()
        kubernetesClient.pods().create(podWithAttachedContainer)
        newlyCreatedExecutors(newExecutorId) = clock.getTimeMillis()
        logDebug(s"Requested executor with id $newExecutorId from Kubernetes.")
      }
    }

    deletedExecutorIds = _deletedExecutorIds

    // Update the flag that helps the setTotalExpectedExecutors() callback avoid triggering this
    // update method when not needed. PODs known by the scheduler backend are not counted here as
    // they considered running PODs and they should not block upscaling.
    numOutstandingPods.set(knownPendingCount + newlyCreatedExecutors.size)

    // The code below just prints debug messages, which are only useful when there's a change
    // in the snapshot state. Since the messages are a little spammy, avoid them when we know
    // there are no useful updates.
    if (!log.isDebugEnabled || snapshots.isEmpty) {
      return
    }

    if (currentRunningCount >= currentTotalExpectedExecutors && !dynamicAllocationEnabled) {
      logDebug("Current number of running executors is equal to the number of requested" +
        " executors. Not scaling up further.")
    } else {
      val outstanding = knownPendingCount + newlyCreatedExecutors.size
      if (outstanding > 0) {
        logDebug(s"Still waiting for $outstanding executors before requesting more.")
      }
    }
  }

  private def isExecutorIdleTimedOut(state: ExecutorPodState, currentTime: Long): Boolean = {
    try {
      val startTime = Instant.parse(state.pod.getStatus.getStartTime).toEpochMilli()
      currentTime - startTime > executorIdleTimeout
    } catch {
      case _: Exception =>
        logDebug(s"Cannot get startTime of pod ${state.pod}")
        true
    }
  }
}
