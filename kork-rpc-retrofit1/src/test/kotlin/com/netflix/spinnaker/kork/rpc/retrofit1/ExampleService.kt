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
package com.netflix.spinnaker.kork.rpc.retrofit1

import com.netflix.spinnaker.kork.rpc.client.Bulkhead
import com.netflix.spinnaker.kork.rpc.client.Cache
import com.netflix.spinnaker.kork.rpc.client.CacheBehavior
import com.netflix.spinnaker.kork.rpc.client.CacheEvictionPolicy
import com.netflix.spinnaker.kork.rpc.client.CacheSizeType
import com.netflix.spinnaker.kork.rpc.client.CircuitBreaker
import com.netflix.spinnaker.kork.rpc.client.Debounce
import com.netflix.spinnaker.kork.rpc.client.RateLimit
import com.netflix.spinnaker.kork.rpc.client.Retry
import com.netflix.spinnaker.kork.rpc.client.TimeLimit
import retrofit.http.Body
import retrofit.http.GET
import retrofit.http.Headers
import retrofit.http.POST
import retrofit.http.Path
import retrofit.http.Query
import java.util.function.Predicate

/**
 * In practice, we could either define things at the service- or method-level for
 * various behaviors, or use defaults supplied by the framework.
 *
 * This is an exhibit of how we could add these annotations onto an existing Retrofit
 * service. There's some non-committed code that adds a higher-level abstraction that
 * hides service implementation (meaning, we could use Retrofit1 in one spot, Retrofit2
 * in another, or gRPC in yet another spot but have the same client-facing code). I
 * didn't include it in this commit because it doesn't work yet, but that's the end
 * goal I'm shooting for.
 */
@Retry(maxAttempts=3, backoffMs=100)
@RateLimit(rps=10, waitTimeMs=100)
@TimeLimit(timeoutMs=5_000)
@Bulkhead(maxConcurrentCalls=10, maxWaitTimeMs=1000)
@CircuitBreaker(
  failureRateThreshold = 50,
  waitDurationInOpenStateMs = 100,
  ringBufferSizeInClosedState = 2,
  ringBufferSizeInHalfOpenState = 2,
  failurePredicate = MyCircuitBreakerPredicate::class
)
interface ExampleService {

  @Retry(maxAttempts = 5, backoffMs = 100)
  @Headers("Content-type: application/context+json")
  @POST("/ops")
  fun doOperation(@Body body: Map<String, *>): Map<Any, Any>

  @Debounce(maxDelayMs = 50)
  @Headers("Accept: application/json")
  @GET("/applications/{application}/tasks")
  fun getTasks(@Path("application") app: String,
               @Query("page") page: Int,
               @Query("limit") limit: Int,
               @Query("statuses") statuses: String): List<Any>

  @Debounce(maxDelayMs = 50)
  @Headers("Accept: application/json")
  @GET("/v2/applications/{application}/pipelines")
  fun getPipelines(@Path("application") app: String,
                   @Query("limit") limit: Int,
                   @Query("statuses") statuses: String,
                   @Query("expand") expand: Boolean): List<Any>

  @Cache(
    behavior = CacheBehavior.CACHE_FIRST,
    evictionPolicy = CacheEvictionPolicy.RECENCY,
    maxAgeMs = 60_000,
    maxSize = 20,
    maxSizeType = CacheSizeType.ELEMENTS,
    predicate = MyCachingPredicate::class
  )
  @Debounce(maxDelayMs=500)
  @Headers("Accept: application/json")
  @GET("/projects/{projectId}/pipelines")
  fun getPipelinesForProject(@Path("projectId") projectId: String,
                             @Query("limit") limit: Int,
                             @Query("statuses") statuses: String): List<Map<Any, Any>>
}

class MyCachingPredicate : Predicate<Any> {
  override fun test(t: Any): Boolean {
    throw UnsupportedOperationException("not implemented")
  }
}

class MyCircuitBreakerPredicate : Predicate<Any> {
  override fun test(t: Any): Boolean {
    throw UnsupportedOperationException("not implemented")
  }

}
