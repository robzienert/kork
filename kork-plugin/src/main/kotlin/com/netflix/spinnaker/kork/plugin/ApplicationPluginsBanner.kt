/*
 * Copyright 2019 Netflix, Inc.
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
package com.netflix.spinnaker.kork.plugin

import org.springframework.boot.Banner
import org.springframework.core.env.Environment
import java.io.PrintStream

/**
 * Amends loaded plugin information to the application banner.
 */
class ApplicationPluginsBanner(
  private val plugins: List<ApplicationPlugin<*>>,
  private val verbose: Boolean
) : Banner {

  override fun printBanner(environment: Environment, sourceClass: Class<*>, out: PrintStream) {
    if (plugins.isEmpty()) {
      return
    }

    out.println("Spinnaker Plugins")
    plugins.forEach { plugin ->
      if (verbose) {
        out.println("${plugin.fingerprint}: ${enabledFlag(plugin.userConfig)}")
        out.row("Name", plugin.name)
        out.row("Author", "${plugin.owner}, ${plugin.email}")
        out.row("Website", plugin.website)
        out.row("Repository", plugin.repository)
        out.println("-".repeat(20))
      } else {
        out.println("${plugin.fingerprint}: ${enabledFlag(plugin.userConfig)}")
      }
    }
  }

  private fun PrintStream.row(name: String, value: String) {
    println("${name.take(10).padEnd(10)}: $value")
  }

  private fun enabledFlag(config: ApplicationPluginConfig?) =
    if (config == null || config.enabled) {
      "ENABLED"
    } else {
      "DISABLED"
    }
}
