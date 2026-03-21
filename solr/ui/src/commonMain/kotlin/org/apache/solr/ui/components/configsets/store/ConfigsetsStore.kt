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
import org.apache.solr.ui.components.configsets.data.ListConfigsets
import org.apache.solr.ui.components.configsets.store.ConfigsetsStore.Intent
import org.apache.solr.ui.components.configsets.store.ConfigsetsStore.State

internal interface ConfigsetsStore : Store<Intent, State, Nothing> {

    sealed interface Intent {
        /**
         * Intent for selecting configset.
         *
         * @property configSetName Name of the configset to select.
         * @property reload Whether to reload the configset before selecting.
         */
        data class SelectConfigset(
            val configSetName: String,
            val reload: Boolean = false,
        ) : Intent
    }

    data class State(
        val selectedConfigset: String? = null,
        val configSets: ListConfigsets = ListConfigsets(),
    )
}
