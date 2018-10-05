/*
 * Copyright 2018 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.spinnaker.kork.rpc.client

import java.util.function.Predicate
import kotlin.annotation.AnnotationRetention.RUNTIME
import kotlin.annotation.AnnotationTarget.CLASS
import kotlin.annotation.AnnotationTarget.FUNCTION
import kotlin.reflect.KClass

const val SERVICE_GROUP = "ServiceGroup"

/**
 * An annotation that allows overriding the group of a service or method.
 * This can be valuable for bucketing particular service calls into their
 * own resource space.
 */
@MustBeDocumented
@Target(CLASS, FUNCTION)
@Retention(RUNTIME)
annotation class RadioId(
  val name: String
)

/**
 * Provides call retries.
 */
@MustBeDocumented
@Target(CLASS, FUNCTION)
@Retention(RUNTIME)
annotation class Retry(
  val maxAttempts: Int,
  val backoffMs: Long
)

/**
 * Puts a limitation on the rate of invocations allowed for a call or service.
 *
 * The provided [rps] is scoped to the level this annotation is attached. If
 * attached to the service, this value is compared to the aggregate number of
 * service requests.
 *
 * Once a rate limited request's [waitTimeMs] has been exceeded, it will fail
 * with a [RateLimitExceeded] error.
 *
 * This rate limiter is per-object and uncoordinated with other clients both
 * in-process and remote.
 */
@MustBeDocumented
@Target(CLASS, FUNCTION)
@Retention(RUNTIME)
annotation class RateLimit(
  val rps: Int,
  val waitTimeMs: Long = 0
)

/**
 * Delays execution of requests for a [maxDelayMs], batching service method
 * calls that arrive within the delay window.
 *
 * The delay window is set on the first request received for a particular
 * method & argument combination and flushed at the end. A request will not
 * be delayed longer than this maximum amount.
 */
@MustBeDocumented
@Target(CLASS, FUNCTION)
@Retention(RUNTIME)
annotation class Debounce(
  val maxDelayMs: Long
)

/**
 * Sets a timelimit on request fulfillment.
 *
 * The [timeoutMs] value is used to pass a response deadline for the request
 * to the server, which it will cascade to its own child requests.
 */
@MustBeDocumented
@Target(CLASS, FUNCTION)
@Retention(RUNTIME)
annotation class TimeLimit(
  val timeoutMs: Long
)

/**
 * Opens a circuit breaker in excessive downstream service errors.
 */
@MustBeDocumented
@Target(CLASS, FUNCTION)
@Retention(RUNTIME)
annotation class CircuitBreaker(
  val group: String = SERVICE_GROUP,
  val failureRateThreshold: Int,
  val waitDurationInOpenStateMs: Long,
  val ringBufferSizeInHalfOpenState: Int,
  val ringBufferSizeInClosedState: Int,
  val failurePredicate: KClass<out Predicate<*>>
)

/**
 * Limits the parallel number of executions against a service or method.
 *
 * A call will block for up to [maxWaitTimeMs] if the number of concurrent
 * calls to a service or method exceeds [maxConcurrentCalls].
 */
@MustBeDocumented
@Target(CLASS, FUNCTION)
@Retention(RUNTIME)
annotation class Bulkhead(
  val maxConcurrentCalls: Int,
  val maxWaitTimeMs: Long
)

/**
 * Caches the response in-memory.
 *
 * TODO rz - Do we want this one? Could be handy, but low pri.
 */
@MustBeDocumented
@Target(CLASS, FUNCTION)
@Retention(RUNTIME)
annotation class Cache(
  val behavior: CacheBehavior = CacheBehavior.CACHE_FIRST,
  val evictionPolicy: CacheEvictionPolicy = CacheEvictionPolicy.RECENCY,
  val maxAgeMs: Long,
  val maxSize: Long,
  val maxSizeType: CacheSizeType = CacheSizeType.ELEMENTS,
  val predicate: KClass<out Predicate<*>>
)

enum class CacheBehavior {
  CACHE_ONLY,
  CACHE_FIRST,
  NETWORK_FIRST,
  NETWORK_ONLY
}

enum class CacheEvictionPolicy {
  RECENCY,
  FREQUENCY
}

enum class CacheSizeType {
  MEMORY,
  ELEMENTS
}
