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

package org.apache.solr.ui.components.configsets.viewmodel

import androidx.compose.runtime.mutableStateListOf
import androidx.compose.runtime.snapshots.SnapshotStateList
import androidx.lifecycle.ViewModel
import androidx.navigation3.runtime.NavKey
import kotlinx.serialization.Serializable
import org.apache.solr.ui.components.configsets.viewmodel.ConfigsetsOverviewEntry.ConfigsetsOverviewDialog.CreateConfigsetDialog
import org.apache.solr.ui.components.configsets.viewmodel.ConfigsetsOverviewEntry.ConfigsetsOverviewDialog.ImportConfigsetDialog

class ConfigsetsOverviewViewModel : ViewModel() {

    val backStack: SnapshotStateList<ConfigsetsOverviewEntry> =
        mutableStateListOf(ConfigsetsOverviewEntry.Main)

    /**
     * Initiates the creation of a new configset.
     */
    fun openCreateConfigsetDialog() = backStack.apply {
        clearDialogs()
        add(CreateConfigsetDialog)
    }

    /**
     * Initiates the import of a configset.
     */
    fun openImportConfigsetDialog() = backStack.apply {
        clearDialogs()
        add(ImportConfigsetDialog)
    }

    /**
     * Closes any opened dialog.
     */
    fun closeDialog() {
        backStack.clearDialogs()
    }

    /**
     * Edit solrconfig.xml for the configset with the given [name].
     *
     * @param name the name of the configset to edit.
     */
    fun editSolrConfig(name: String) {
        TODO()
    }

    private fun SnapshotStateList<ConfigsetsOverviewEntry>.clearDialogs() = removeAll { entry -> entry is ConfigsetsOverviewEntry.ConfigsetsOverviewDialog }
}

@Serializable
sealed interface ConfigsetsOverviewEntry : NavKey {

    @Serializable
    data object Main : ConfigsetsOverviewEntry

    @Serializable
    sealed interface ConfigsetsOverviewDialog : ConfigsetsOverviewEntry {

        @Serializable
        data object CreateConfigsetDialog : ConfigsetsOverviewDialog

        @Serializable
        data object ImportConfigsetDialog : ConfigsetsOverviewDialog
    }
}
