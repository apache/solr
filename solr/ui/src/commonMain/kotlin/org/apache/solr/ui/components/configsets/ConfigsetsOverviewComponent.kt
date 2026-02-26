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

import com.arkivanov.decompose.router.slot.ChildSlot
import com.arkivanov.decompose.value.Value
import kotlinx.serialization.Serializable

interface ConfigsetsOverviewComponent {

    val dialog: Value<ChildSlot<CreateConfigsetDialogConfig, *>>

    /**
     * Initiates the creation of a new configset.
     */
    fun createConfigset()

    /**
     * Initiates the import of a configset.
     */
    fun importConfigset()

    /**
     * Closes any opened dialog.
     */
    fun closeDialog()

    /**
     * Edit solrconfig.xml for the configset with the given [name].
     *
     * @param name the name of the configset to edit.
     */
    fun editSolrConfig(name: String)

    @Serializable
    sealed interface CreateConfigsetDialogConfig {

        @Serializable
        data object CreateConfigsetWithInputDialogConfig : CreateConfigsetDialogConfig

        @Serializable
        data object ImportConfigsetDialogConfig : CreateConfigsetDialogConfig
    }
}
