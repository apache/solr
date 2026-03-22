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

package org.apache.solr.ui.views.configsets.actionbars

import androidx.compose.foundation.layout.RowScope
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.width
import androidx.compose.material3.Icon
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.ui.Modifier
import androidx.compose.ui.unit.dp
import org.apache.solr.ui.generated.resources.Res
import org.apache.solr.ui.generated.resources.action_create_configset
import org.apache.solr.ui.generated.resources.action_edit_solrconfig
import org.apache.solr.ui.generated.resources.add
import org.apache.solr.ui.generated.resources.edit
import org.apache.solr.ui.views.components.SolrTextButton
import org.jetbrains.compose.resources.painterResource
import org.jetbrains.compose.resources.stringResource

@Composable
fun RowScope.ConfigsetsOverviewMainActions(
    onCreateConfigset: () -> Unit,
    onEditSolrConfig: (configset: String) -> Unit,
    selectedConfigset: String? = null,
) {
    SolrTextButton(onClick = onCreateConfigset) {
        Icon(painter = painterResource(Res.drawable.add), contentDescription = null)
        Spacer(modifier = Modifier.width(8.dp))
        Text(stringResource(Res.string.action_create_configset))
    }
    if (!selectedConfigset.isNullOrBlank()) {
        SolrTextButton(onClick = { onEditSolrConfig(selectedConfigset) }) {
            Icon(painter = painterResource(Res.drawable.edit), contentDescription = null)
            Spacer(modifier = Modifier.width(8.dp))
            Text(stringResource(Res.string.action_edit_solrconfig))
        }
    }
}
