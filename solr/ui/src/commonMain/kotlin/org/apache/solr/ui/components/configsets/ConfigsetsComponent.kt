/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.ui.components.configsets

import kotlinx.coroutines.flow.StateFlow
import org.apache.solr.ui.domain.Configset

/**
 * The configsets component keeps record of the currently available configsets, and a selected
 * configset that may be used for additional operations.
 */
interface ConfigsetsComponent {

    /**
     * Model that holds the data of the [ConfigsetsComponent].
     *
     * @property configsets The configsets names available.
     * @property selectedConfigset The current configset name to display. Leave empty if no
     * selection is made.
     */
    data class Model(
        val configsets: List<Configset> = emptyList(),
        val selectedConfigset: String = "",
    )

    /** Hot, observable stream of [Model] for Compose/UI. */
    val model: StateFlow<Model>

    /**
     * Select the active configset by name.
     *
     * @param name The name of the configset to select.
     * @param reload Whether to reload the list before selecting the configset. This is useful when
     * the configset has newly been added and the list has to be reloaded.
     */
    fun onSelectConfigset(name: String, reload: Boolean = false)
}
