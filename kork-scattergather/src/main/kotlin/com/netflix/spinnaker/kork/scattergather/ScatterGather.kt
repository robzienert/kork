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
package com.netflix.spinnaker.kork.scattergather

import kotlinx.coroutines.experimental.TimeoutCancellationException
import kotlinx.coroutines.experimental.async
import kotlinx.coroutines.experimental.runBlocking
import kotlinx.coroutines.experimental.withTimeout
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.atomic.AtomicLong

/**
 * Missing:
 *
 * - Retries (if we get a 429 from a service _and_ we haven't timed out, we may as well
 *    give it another go)
 * - Metrics
 */
class ScatterGather {

  private val log = LoggerFactory.getLogger(javaClass)

  fun <T, R> perform(
    context: OperationContext,
    inputs: List<ScatterInput>,
    scatter: (ScatterInput) -> T?,
    gather: (List<T?>) -> R
  ): R {

    val metrics = OperationMetrics(totalOperations = inputs.size.toLong())

    return runBlocking {
      val operations = inputs.map { input ->
        Pair(input, async { scatter(input) })
      }

      val results = operations.map { operation ->
        try {
          withTimeout(context.timeout.toMillis()) {
            operation.second.await()
          }
        } catch (e: TimeoutCancellationException) {
          log.error("Timeout while waiting for scatter operation ${context.id}/${operation.first.id} to complete")
          metrics.scatterTimeouts.incrementAndGet()
          null
        }
      }

      gather(results)
    }
  }

}

data class OperationContext(
  val id: String,
  val timeout: Duration
)

data class OperationMetrics(
  val totalOperations: Long,
  val scatterTimeouts: AtomicLong = AtomicLong(0),
  val totalTiming: AtomicLong = AtomicLong(0)
)

interface ScatterInput {
  val id: String
}

data class DefaultScatterInput(
  override val id: String
) : ScatterInput
