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

import org.springframework.boot.builder.SpringApplicationBuilder
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer

/**
 * Offers a standard method of including plugins and extensions into Spinnaker without requiring users to create
 * custom builds of a particular service. These plugins are only loaded on startup and cannot be dynamically
 * loaded or reloaded without a process restart.
 *
 * TODO(rz): Allow services to define minimum versions or disable plugin loading.
 * TODO(rz): Userland config to define what packages to scan for components.
 * TODO(rz): Current design allows plugins to hang out on the same classloader - add a classloader sandbox for each to
 * (optionally) run certain code in a different classloader?
 * TODO(rz): Add an Endpoint that exposes info of all the loaded plugins
 */
class ApplicationPluginLoader(
  private val springApplicationBuilder: SpringApplicationBuilder
) : SpringBootServletInitializer() {

  override fun configure(builder: SpringApplicationBuilder): SpringApplicationBuilder {
    // TODO(rz): Should add spring-plugin and use that to locate all of the extensions?
    val plugins = listOf<ApplicationPlugin<*>>()

    return builder.banner(ApplicationPluginsBanner(plugins, VERBOSE))
  }

  companion object {
    val VERBOSE = System.getProperty("SPINNAKER_PLUGIN_VERBOSE_STARTUP")?.toBoolean() ?: false
  }
}
