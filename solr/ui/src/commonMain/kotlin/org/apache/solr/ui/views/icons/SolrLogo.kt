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

package org.apache.solr.ui.views.icons

import androidx.compose.foundation.Image
import androidx.compose.foundation.isSystemInDarkTheme
import androidx.compose.runtime.Composable
import androidx.compose.ui.Modifier
import org.apache.solr.ui.generated.resources.Res
import org.apache.solr.ui.generated.resources.cd_solr_logo
import org.apache.solr.ui.generated.resources.solr_logo_dark
import org.apache.solr.ui.generated.resources.solr_logo_light
import org.jetbrains.compose.resources.painterResource
import org.jetbrains.compose.resources.stringResource

/**
 * Colorized Solr logo.
 *
 * @param modifier Modifier that is applied to the wrapper of the logo.
 * @param useDarkTheme Whether to use the dark-themed solr logo, or the light theme.
 */
@Composable
fun SolrLogo(
    modifier: Modifier = Modifier,
    useDarkTheme: Boolean = isSystemInDarkTheme(),
) = Image(
    modifier = modifier,
    painter = painterResource(
        if (useDarkTheme) {
            Res.drawable.solr_logo_dark
        } else {
            Res.drawable.solr_logo_light
        },
    ),
    contentDescription = stringResource(Res.string.cd_solr_logo),
)
