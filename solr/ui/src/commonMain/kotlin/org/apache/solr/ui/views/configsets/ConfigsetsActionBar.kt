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

package org.apache.solr.ui.views.configsets

import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.RowScope
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.width
import androidx.compose.foundation.layout.widthIn
import androidx.compose.material3.Icon
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.unit.dp
import com.arkivanov.decompose.extensions.compose.subscribeAsState
import org.apache.solr.ui.components.configsets.ConfigsetsOverviewComponent
import org.apache.solr.ui.components.configsets.ConfigsetsOverviewComponent.CreateConfigsetDialogConfig
import org.apache.solr.ui.components.configsets.ConfigsetsRouteComponent
import org.apache.solr.ui.components.configsets.CreateConfigsetComponent
import org.apache.solr.ui.components.configsets.ImportConfigsetComponent
import org.apache.solr.ui.domain.Configset
import org.apache.solr.ui.generated.resources.Res
import org.apache.solr.ui.generated.resources.action_create_configset
import org.apache.solr.ui.generated.resources.action_edit_solrconfig
import org.apache.solr.ui.generated.resources.add
import org.apache.solr.ui.generated.resources.edit
import org.apache.solr.ui.views.components.SolrTextButton
import org.jetbrains.compose.resources.painterResource
import org.jetbrains.compose.resources.stringResource

@Composable
fun ConfigsetsActionBar(
    variant: ConfigsetsRouteComponent.Child,
    configsets: List<Configset>,
    onConfigsetChange: (String) -> Unit,
    modifier: Modifier = Modifier,
    selectedConfigset: String = "",
) = Row(
    modifier = modifier,
    horizontalArrangement = Arrangement.spacedBy(8.dp),
    verticalAlignment = Alignment.CenterVertically,
) {
    ConfigsetsDropdown(
        availableConfigsets = configsets,
        selectedConfigSet = selectedConfigset,
        selectConfigset = onConfigsetChange,
        modifier = Modifier.widthIn(min = 128.dp, max = 256.dp),
    )

    when (variant) {
        is ConfigsetsRouteComponent.Child.Overview ->
            ConfigsetsOverviewMainActions(
                component = variant.component,
                selectedConfigset = selectedConfigset,
            )

        // TODO Implement me
        is ConfigsetsRouteComponent.Child.Placeholder -> Text(text = variant.tabName)
    }

    // TODO Add additional actions for pending states, like apply or reset
}

@Composable
private fun RowScope.ConfigsetsOverviewMainActions(
    component: ConfigsetsOverviewComponent,
    selectedConfigset: String? = null,
) {
    val dialog by component.dialog.subscribeAsState()

    SolrTextButton(onClick = component::createConfigset) {
        Icon(painter = painterResource(Res.drawable.add), contentDescription = null)
        Spacer(modifier = Modifier.width(8.dp))
        Text(stringResource(Res.string.action_create_configset))
    }
    if (!selectedConfigset.isNullOrBlank()) {
        SolrTextButton(onClick = { component.editSolrConfig(name = selectedConfigset) }) {
            Icon(painter = painterResource(Res.drawable.edit), contentDescription = null)
            Spacer(modifier = Modifier.width(8.dp))
            Text(stringResource(Res.string.action_edit_solrconfig))
        }
    }

    dialog.child?.let { child ->
        when (child.configuration) {
            CreateConfigsetDialogConfig.CreateConfigsetWithInputDialogConfig ->
                CreateConfigsetDialog(
                    modifier = Modifier.padding(all = 16.dp),
                    component = child.instance as CreateConfigsetComponent,
                    onImport = component::importConfigset,
                    onDismissRequest = component::closeDialog,
                )
            CreateConfigsetDialogConfig.ImportConfigsetDialogConfig -> ImportConfigsetDialog(
                modifier = Modifier.padding(all = 16.dp),
                component = child.instance as ImportConfigsetComponent,
                onCreate = component::createConfigset,
                onDismissRequest = component::closeDialog,
            )
        }
    }
}
