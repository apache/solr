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
import org.apache.solr.ui.components.configsets.store.ImportConfigsetStore.Intent
import org.apache.solr.ui.components.configsets.store.ImportConfigsetStore.Label
import org.apache.solr.ui.components.configsets.store.ImportConfigsetStore.State
import org.apache.solr.ui.domain.Configset
import org.apache.solr.ui.domain.PickedFile

internal interface ImportConfigsetStore : Store<Intent, State, Label> {

    sealed interface Intent {
        data class ChangeConfigsetName(val configsetName: String) : Intent

        data class ImportConfigset(val file: PickedFile) : Intent
    }

    /**
     * @property name The name to use for the configset. Normally populated from the file name.
     * @property isLoading Whether the import is currently in progress.
     */
    data class State(
        val name: String = "",
        val isNameEdited: Boolean = false,
        val isLoading: Boolean = false,
    )

    sealed interface Label {
        data class ConfigsetImported(val configset: Configset) : Label
    }
}
