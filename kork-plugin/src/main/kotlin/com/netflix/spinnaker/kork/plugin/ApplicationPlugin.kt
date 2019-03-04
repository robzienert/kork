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

/**
 * The application plugin model.
 *
 * TODO(rz): add a requirements m<s, s> that would allow plugin owners to require minimum versions of a particular
 * spinnaker service version.
 *
 * @property namespace The kebab-case namespace of the plugin (typically a github username or organization)
 * @property id The kebab-case, human-readable identifier for the plugin within the namespace
 * @property name The name of the application plugin
 * @property owner Name of the plugin owner
 * @property email Email address of the plugin owner
 * @property website The plugin or owner website
 * @property repository The plugin repository
 * @property description A short description of what the plugin does
 * @property version The version of the plugin
 * @property scanPackages The Java packages that should be scanned for plugin composnents
 * @property userConfig The user-defined, runtime config of the application plugin
 */
data class ApplicationPlugin<T : ApplicationPluginConfig>(
  val namespace: String,
  val id: String,
  val name: String,
  val owner: String,
  val email: String,
  val website: String,
  val repository: String,
  val description: String,
  val version: String,
  val scanPackages: List<String>,
  val requirements: Map<String, String>,
  var userConfig: T? = null
) {
  val fingerprint = "$namespace/$id@$version"
}

// TODO(rz): maybe better to create a config registry that you lookup by application plugin, then the models will
// stay immutable.
interface ApplicationPluginConfig {
  val enabled: Boolean
}
