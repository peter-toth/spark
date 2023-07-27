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

package org.apache.spark.sql.connect.service

import java.net.InetSocketAddress
import java.util.UUID
import java.util.concurrent.TimeUnit

import com.google.common.base.Ticker
import com.google.common.cache.{CacheBuilder, RemovalListener, RemovalNotification}
import io.grpc.Server
import io.grpc.netty.NettyServerBuilder
import io.grpc.protobuf.services.ProtoReflectionService
import io.grpc.stub.StreamObserver
import org.apache.commons.lang3.StringUtils

import org.apache.spark.{SparkEnv, SparkSQLException}
import org.apache.spark.connect.proto
import org.apache.spark.connect.proto.{AddArtifactsRequest, AddArtifactsResponse}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connect.config.Connect.{CONNECT_GRPC_BINDING_ADDRESS, CONNECT_GRPC_BINDING_PORT, CONNECT_GRPC_MAX_INBOUND_MESSAGE_SIZE}
import org.apache.spark.sql.connect.utils.ErrorUtils

/**
 * The SparkConnectService implementation.
 *
 * This class implements the service stub from the generated code of GRPC.
 *
 * @param debug
 *   delegates debug behavior to the handlers.
 */
class SparkConnectService(debug: Boolean)
    extends proto.SparkConnectServiceGrpc.SparkConnectServiceImplBase
    with Logging {

  /**
   * This is the main entry method for Spark Connect and all calls to execute a plan.
   *
   * The plan execution is delegated to the [[SparkConnectExecutePlanHandler]]. All error handling
   * should be directly implemented in the deferred implementation. But this method catches
   * generic errors.
   *
   * @param request
   * @param responseObserver
   */
  override def executePlan(
      request: proto.ExecutePlanRequest,
      responseObserver: StreamObserver[proto.ExecutePlanResponse]): Unit = {
    try {
      new SparkConnectExecutePlanHandler(responseObserver).handle(request)
    } catch {
      ErrorUtils.handleError(
        "execute",
        observer = responseObserver,
        userId = request.getUserContext.getUserId,
        sessionId = request.getSessionId)
    }
  }

  /**
   * Analyze a plan to provide metadata and debugging information.
   *
   * This method is called to generate the explain plan for a SparkConnect plan. In its simplest
   * implementation, the plan that is generated by the [[SparkConnectPlanner]] is used to build a
   * [[Dataset]] and derive the explain string from the query execution details.
   *
   * Errors during planning are returned via the [[StreamObserver]] interface.
   *
   * @param request
   * @param responseObserver
   */
  override def analyzePlan(
      request: proto.AnalyzePlanRequest,
      responseObserver: StreamObserver[proto.AnalyzePlanResponse]): Unit = {
    try {
      new SparkConnectAnalyzeHandler(responseObserver).handle(request)
    } catch {
      ErrorUtils.handleError(
        "analyze",
        observer = responseObserver,
        userId = request.getUserContext.getUserId,
        sessionId = request.getSessionId)
    }
  }

  /**
   * This is the main entry method for Spark Connect and all calls to update or fetch
   * configuration..
   *
   * @param request
   * @param responseObserver
   */
  override def config(
      request: proto.ConfigRequest,
      responseObserver: StreamObserver[proto.ConfigResponse]): Unit = {
    try {
      new SparkConnectConfigHandler(responseObserver).handle(request)
    } catch {
      ErrorUtils.handleError(
        "config",
        observer = responseObserver,
        userId = request.getUserContext.getUserId,
        sessionId = request.getSessionId)
    }
  }

  /**
   * This is the main entry method for all calls to add/transfer artifacts.
   *
   * @param responseObserver
   * @return
   */
  override def addArtifacts(responseObserver: StreamObserver[AddArtifactsResponse])
      : StreamObserver[AddArtifactsRequest] = new SparkConnectAddArtifactsHandler(
    responseObserver)

  /**
   * This is the entry point for all calls of getting artifact statuses.
   */
  override def artifactStatus(
      request: proto.ArtifactStatusesRequest,
      responseObserver: StreamObserver[proto.ArtifactStatusesResponse]): Unit = {
    try {
      new SparkConnectArtifactStatusesHandler(responseObserver).handle(request)
    } catch
      ErrorUtils.handleError(
        "artifactStatus",
        observer = responseObserver,
        userId = request.getUserContext.getUserId,
        sessionId = request.getSessionId)
  }

  /**
   * This is the entry point for calls interrupting running executions.
   */
  override def interrupt(
      request: proto.InterruptRequest,
      responseObserver: StreamObserver[proto.InterruptResponse]): Unit = {
    try {
      new SparkConnectInterruptHandler(responseObserver).handle(request)
    } catch
      ErrorUtils.handleError(
        "interrupt",
        observer = responseObserver,
        userId = request.getUserContext.getUserId,
        sessionId = request.getSessionId)
  }
}

/**
 * Static instance of the SparkConnectService.
 *
 * Used to start the overall SparkConnect service and provides global state to manage the
 * different SparkSession from different users connecting to the cluster.
 */
object SparkConnectService extends Logging {

  private val CACHE_SIZE = 100

  private val CACHE_TIMEOUT_SECONDS = 3600

  // Type alias for the SessionCacheKey. Right now this is a String but allows us to switch to a
  // different or complex type easily.
  private type SessionCacheKey = (String, String)

  private[connect] var server: Server = _

  // For testing purpose, it's package level private.
  private[connect] def localPort: Int = {
    assert(server != null)
    // Return the actual local port being used. This can be different from the csonfigured port
    // when the server binds to the port 0 as an example.
    server.getPort
  }

  private val userSessionMapping =
    cacheBuilder(CACHE_SIZE, CACHE_TIMEOUT_SECONDS).build[SessionCacheKey, SessionHolder]()

  private[connect] val streamingSessionManager =
    new SparkConnectStreamingQueryCache(sessionKeepAliveFn = { case (userId, sessionId) =>
      // Use getIfPresent() rather than get() to prevent accidental loading.
      userSessionMapping.getIfPresent((userId, sessionId))
    })

  private class RemoveSessionListener extends RemovalListener[SessionCacheKey, SessionHolder] {
    override def onRemoval(
        notification: RemovalNotification[SessionCacheKey, SessionHolder]): Unit = {
      notification.getValue.expireSession()
    }
  }

  // Simple builder for creating the cache of Sessions.
  private def cacheBuilder(cacheSize: Int, timeoutSeconds: Int): CacheBuilder[Object, Object] = {
    var cacheBuilder = CacheBuilder.newBuilder().ticker(Ticker.systemTicker())
    if (cacheSize >= 0) {
      cacheBuilder = cacheBuilder.maximumSize(cacheSize)
    }
    if (timeoutSeconds >= 0) {
      cacheBuilder.expireAfterAccess(timeoutSeconds, TimeUnit.SECONDS)
    }
    cacheBuilder.removalListener(new RemoveSessionListener)
    cacheBuilder
  }

  /**
   * Based on the `key` find or create a new SparkSession.
   */
  def getOrCreateIsolatedSession(userId: String, sessionId: String): SessionHolder = {
    // Validate that sessionId is formatted like UUID before creating session.
    try {
      UUID.fromString(sessionId).toString
    } catch {
      case _: IllegalArgumentException =>
        throw new SparkSQLException(
          errorClass = "INVALID_HANDLE.FORMAT",
          messageParameters = Map("handle" -> sessionId))
    }
    userSessionMapping.get(
      (userId, sessionId),
      () => {
        val holder = SessionHolder(userId, sessionId, newIsolatedSession())
        holder.initializeSession()
        holder
      })
  }

  /**
   * Used for testing
   */
  private[connect] def invalidateAllSessions(): Unit = {
    userSessionMapping.invalidateAll()
  }

  private def newIsolatedSession(): SparkSession = {
    SparkSession.active.newSession()
  }

  /**
   * Starts the GRPC Serivce.
   */
  private def startGRPCService(): Unit = {
    val debugMode = SparkEnv.get.conf.getBoolean("spark.connect.grpc.debug.enabled", true)
    val bindAddress = SparkEnv.get.conf.get(CONNECT_GRPC_BINDING_ADDRESS)
    val port = SparkEnv.get.conf.get(CONNECT_GRPC_BINDING_PORT)
    val sb = bindAddress match {
      case Some(hostname) =>
        logInfo(s"start GRPC service at: $hostname")
        NettyServerBuilder.forAddress(new InetSocketAddress(hostname, port))
      case _ => NettyServerBuilder.forPort(port)
    }
    sb.maxInboundMessageSize(SparkEnv.get.conf.get(CONNECT_GRPC_MAX_INBOUND_MESSAGE_SIZE).toInt)
      .addService(new SparkConnectService(debugMode))

    // Add all registered interceptors to the server builder.
    SparkConnectInterceptorRegistry.chainInterceptors(sb)

    // If debug mode is configured, load the ProtoReflection service so that tools like
    // grpcurl can introspect the API for debugging.
    if (debugMode) {
      sb.addService(ProtoReflectionService.newInstance())
    }
    server = sb.build
    server.start()
  }

  // Starts the service
  def start(): Unit = {
    startGRPCService()
  }

  def stop(timeout: Option[Long] = None, unit: Option[TimeUnit] = None): Unit = {
    if (server != null) {
      if (timeout.isDefined && unit.isDefined) {
        server.shutdown()
        server.awaitTermination(timeout.get, unit.get)
      } else {
        server.shutdownNow()
      }
    }
    userSessionMapping.invalidateAll()
  }

  def extractErrorMessage(st: Throwable): String = {
    val message = StringUtils.abbreviate(st.getMessage, 2048)
    convertNullString(message)
  }

  def convertNullString(str: String): String = {
    if (str != null) {
      str
    } else {
      ""
    }
  }
}
