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

package org.apache.solr.ui.views.environment

import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.width
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.ui.Modifier
import androidx.compose.ui.text.style.TextAlign
import androidx.compose.ui.unit.dp
import org.apache.solr.ui.components.environment.data.JvmData
import org.apache.solr.ui.components.environment.data.Versions
import org.apache.solr.ui.views.components.SolrCard

/**
 * Composable card that displays system values related to versions.
 *
 * @param versions Solr versions to display.
 * @param jvm JVM values to display.
 * @param modifier Modifier to apply to the root composable.
 */
@Composable
internal fun VersionsCard(
    versions: Versions,
    jvm: JvmData,
    modifier: Modifier = Modifier,
) = SolrCard(
    modifier = modifier,
    verticalArrangement = Arrangement.spacedBy(16.dp),
) {
    Text(
        text = "Versions",
        style = MaterialTheme.typography.headlineSmall,
        color = MaterialTheme.colorScheme.onSurfaceVariant,
    )
    VersionEntry(
        label = "solr-spec",
        value = versions.solrSpecVersion,
    )
    VersionEntry(
        label = "solr-impl",
        value = versions.solrImplVersion,
    )
    VersionEntry(
        label = "lucene-spec",
        value = versions.luceneSpecVersion,
    )
    VersionEntry(
        label = "lucene-impl",
        value = versions.luceneImplVersion,
    )
    VersionEntry(
        label = "Runtime",
        value = "${jvm.name} ${jvm.version}",
    )
}

/**
 * Composable that displays a version entry (key-value pair) in a list.
 */
@Composable
private fun VersionEntry(
    label: String,
    value: String,
) = Row(
    modifier = Modifier.fillMaxWidth(),
    horizontalArrangement = Arrangement.spacedBy(16.dp),
) {
    Text(
        modifier = Modifier.width(96.dp),
        text = label,
        textAlign = TextAlign.End,
        style = MaterialTheme.typography.labelLarge,
        color = MaterialTheme.colorScheme.onSurfaceVariant,
    )
    Text(
        modifier = Modifier.weight(1f),
        text = value,
        style = MaterialTheme.typography.bodyMedium,
        color = MaterialTheme.colorScheme.onSurface,
    )
}
