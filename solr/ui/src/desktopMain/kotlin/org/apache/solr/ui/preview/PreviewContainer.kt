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

package org.apache.solr.ui.preview

import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.padding
import androidx.compose.material3.Surface
import androidx.compose.runtime.Composable
import androidx.compose.ui.Modifier
import androidx.compose.ui.unit.dp
import org.apache.solr.ui.views.theme.SolrTheme

/**
 * This preview container can be used for applying the basic app theme on existing components.
 */
@Composable
fun PreviewContainer(
    modifier: Modifier = Modifier.fillMaxSize(),
    content: @Composable () -> Unit,
) = SolrTheme(useDarkTheme = false) {
    Surface(modifier = modifier) {
        Surface(
            modifier = Modifier.padding(16.dp),
            content = content,
        )
    }
}
