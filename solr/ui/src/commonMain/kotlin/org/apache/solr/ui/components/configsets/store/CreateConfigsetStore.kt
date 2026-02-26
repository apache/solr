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

package org.apache.solr.ui.components.configsets.store

import com.arkivanov.mvikotlin.core.store.Store
import org.apache.solr.ui.components.configsets.store.CreateConfigsetStore.Intent
import org.apache.solr.ui.components.configsets.store.CreateConfigsetStore.Label
import org.apache.solr.ui.components.configsets.store.CreateConfigsetStore.State
import org.apache.solr.ui.domain.Configset

internal interface CreateConfigsetStore : Store<Intent, State, Label> {

    sealed interface Intent {
        data class ChangeConfigsetName(val configsetName: String) : Intent

        data class SelectConfigsetFile(val configsetFile: String) : Intent

        data class ChangeBaseConfigset(val baseConfigset: String?) : Intent

        data object CreateConfigset : Intent
    }

    data class State(
        val configsetName: String = "",
        val baseConfigset: String? = null,
        val configsets: List<Configset> = emptyList(),
        val isLoading: Boolean = false,
    )

    sealed interface Label {
        data class ConfigsetCreated(val configset: Configset) : Label
    }
}
