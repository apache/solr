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

package org.apache.solr.ui.components.files.store

import com.arkivanov.mvikotlin.core.store.Store
import org.apache.solr.ui.components.files.store.FilePickerStore.Intent
import org.apache.solr.ui.components.files.store.FilePickerStore.Label
import org.apache.solr.ui.components.files.store.FilePickerStore.State
import org.apache.solr.ui.domain.PickedFile

internal interface FilePickerStore : Store<Intent, State, Label> {

    sealed interface Intent {
        data object OpenFilePicker : Intent

        data object ClearSelection : Intent
    }

    data class State(
        val selectedFile: PickedFile? = null,
        val isFileSelectionEnabled: Boolean = true,
    )

    sealed interface Label {
        data class FilePicked(val file: PickedFile) : Label
    }
}
