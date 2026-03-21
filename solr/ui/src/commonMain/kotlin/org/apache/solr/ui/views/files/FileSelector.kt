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

package org.apache.solr.ui.views.files

import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.width
import androidx.compose.material3.Icon
import androidx.compose.material3.IconButton
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.ui.Modifier
import androidx.compose.ui.unit.dp
import org.apache.solr.ui.components.files.FilePickerComponent
import org.apache.solr.ui.generated.resources.Res
import org.apache.solr.ui.generated.resources.cd_clear_field
import org.apache.solr.ui.generated.resources.close
import org.apache.solr.ui.generated.resources.upload
import org.apache.solr.ui.views.components.SolrTextButton
import org.jetbrains.compose.resources.painterResource
import org.jetbrains.compose.resources.stringResource

@Composable
fun FileSelector(
    component: FilePickerComponent,
    label: String,
    selectFileText: String,
    modifier: Modifier = Modifier,
) {
    val model by component.model.collectAsState()

    val file = model.selectedFile
    if (file != null) {
        OutlinedTextField(
            modifier = modifier,
            value = file.name,
            label = { Text(label) },
            onValueChange = {},
            readOnly = true,
            singleLine = true,
            leadingIcon = { FileTypeIcon(model.selectedFile?.extension ?: "") },
            trailingIcon = {
                IconButton(onClick = component::clearSelection) {
                    Icon(
                        painter = painterResource(Res.drawable.close),
                        contentDescription = stringResource(Res.string.cd_clear_field),
                    )
                }
            },
        )
    } else {
        SolrTextButton(modifier = modifier, onClick = component::onSelectFile) {
            Icon(painter = painterResource(Res.drawable.upload), contentDescription = null)
            Spacer(modifier = Modifier.width(8.dp))
            Text(text = selectFileText)
        }
    }
}
