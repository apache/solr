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

import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.RowScope
import androidx.compose.foundation.layout.widthIn
import androidx.compose.runtime.Composable
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.unit.dp
import org.apache.solr.ui.domain.Configset
import org.apache.solr.ui.views.configsets.ConfigsetsDropdown

@Composable
fun ConfigsetsActionBar(
    configsets: List<Configset>,
    onConfigsetChange: (String) -> Unit,
    modifier: Modifier = Modifier,
    selectedConfigset: String? = null,
    actions: @Composable RowScope.() -> Unit,
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

    actions()
}
