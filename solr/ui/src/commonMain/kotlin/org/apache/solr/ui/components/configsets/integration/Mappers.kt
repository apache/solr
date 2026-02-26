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

package org.apache.solr.ui.components.configsets.integration

import org.apache.solr.ui.components.configsets.ConfigsetsComponent
import org.apache.solr.ui.components.configsets.CreateConfigsetComponent
import org.apache.solr.ui.components.configsets.ImportConfigsetComponent
import org.apache.solr.ui.components.configsets.store.ConfigsetsStore
import org.apache.solr.ui.components.configsets.store.CreateConfigsetStore
import org.apache.solr.ui.components.configsets.store.ImportConfigsetStore
import org.apache.solr.ui.components.files.store.FilePickerStore
import org.apache.solr.ui.domain.Configset

internal val configsetsStateToModel: (ConfigsetsStore.State) -> ConfigsetsComponent.Model = {
    ConfigsetsComponent.Model(
        configsets = it.configSets.configSets.sorted()
            .map { s -> Configset(s) },
        selectedConfigset = it.selectedConfigset ?: "",
    )
}

internal val createConfigsetStateToModel: (CreateConfigsetStore.State) -> CreateConfigsetComponent.Model = {
    CreateConfigsetComponent.Model(
        configsetName = it.configsetName,
        configsets = it.configsets,
        selectedBaseConfigset = it.baseConfigset ?: "",
        isLoading = it.isLoading,
    )
}

internal val importConfigsetStateToModel: (ImportConfigsetStore.State) -> ImportConfigsetComponent.Model = {
    ImportConfigsetComponent.Model(
        configsetName = it.name,
        isLoading = it.isLoading,
    )
}
