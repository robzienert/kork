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

import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.describe
import org.jetbrains.spek.api.dsl.given
import org.jetbrains.spek.api.dsl.it
import strikt.api.expectThat
import strikt.assertions.hasSize
import java.time.Duration
import java.util.UUID

internal object ScatterGatherSpec : Spek({

  describe("scatter request across multiple clients") {

    given("two services") {
      val subject = ScatterGather()

      val result = subject.perform(
        context = OperationContext(
          id = "myOperation",
          timeout = Duration.ofSeconds(5)
        ),
        inputs = listOf(
          // In this example, the scatter input is just some IDs, but we could have a
          // "ClouddriverServiceScatterInput" which would pass both an ID (the client name, for example?)
          // as well as the actual Retrofit client configured to talk to a particular shard.
          DefaultScatterInput("clouddriver-uswest1"),
          DefaultScatterInput("clouddriver-uswest2")
        ),
        scatter = { input ->
          val region = input.id.split("-")[1]

          // This is where various blocking calls can be made. You could pretend that this
          // particular list is the result of a Retrofit client.
          listOf(
            ServiceResultStub(
              "app1",
              listOf(
                ServerGroup("app1-$region-v001"),
                ServerGroup("app1-$region-v000")
              ),
              mapOf(
                UUID.randomUUID().toString() to input.id,
                "common-key" to "common-value"
              )
            ),
            ServiceResultStub(
              "app2",
              listOf(
                ServerGroup("app2-$region-v001"),
                ServerGroup("app2-$region-v000")
              ),
              mapOf(
                UUID.randomUUID().toString() to input.id,
                "common-key" to "common-value"
              )
            )
          )
        },
        gather = { results ->
          // This is a pretty simple gather operation, where we just perform a shallow merge on the
          // results given by the clients. We're also just throwing away null responses (ones that did not
          // return a result within the provided timeout), but the gather function could also throw an
          // exception if we wanted.
          results
            .asSequence()
            .filterNotNull()
            .fold(mutableListOf<ServiceResultStub>()) { acc, list ->
              acc.addAll(list)
              acc
            }
        }
      )

      it("returns an aggregated list") {
        expectThat(result)
          .hasSize(4)
      }
    }
  }
})

private data class ServiceResultStub(
  val application: String,
  val serverGroups: List<ServerGroup>,
  val mapData: Map<String, String>
)

private data class ServerGroup(
  val name: String
)
