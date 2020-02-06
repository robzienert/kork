/*
 * Copyright 2020 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.netflix.spinnaker.kork.plugins.api;

import com.netflix.spinnaker.kork.annotations.Alpha;

/**
 * Marks an Extension as being capable of accepting a configuration.
 *
 * <p>TODO(rz): Remove entirely. Move to ExtensionConfiguration instead
 *
 * @param <T> The configuration class type
 */
@Alpha
public interface ConfigurableExtension<T> {
  /**
   * Sets the extension configuration. This is called by the plugin framework upon extension
   * creation; it should not be called again by user-land code.
   *
   * @deprecated Configs are now injected via constructors
   */
  @Deprecated
  void setConfiguration(T configuration);
}
