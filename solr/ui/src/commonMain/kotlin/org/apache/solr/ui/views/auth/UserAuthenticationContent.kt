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

package org.apache.solr.ui.views.auth

import androidx.compose.foundation.Image
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.aspectRatio
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.widthIn
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.ChevronLeft
import androidx.compose.material3.ButtonDefaults
import androidx.compose.material3.Icon
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.scale
import androidx.compose.ui.platform.testTag
import androidx.compose.ui.unit.dp
import com.arkivanov.decompose.extensions.compose.subscribeAsState
import org.apache.solr.ui.components.auth.AuthenticationComponent
import org.apache.solr.ui.generated.resources.Res
import org.apache.solr.ui.generated.resources.action_go_back
import org.apache.solr.ui.generated.resources.cd_back_navigation
import org.apache.solr.ui.generated.resources.cd_solr_logo
import org.apache.solr.ui.generated.resources.desc_solr_instance_with_auth
import org.apache.solr.ui.generated.resources.solr_sun
import org.apache.solr.ui.generated.resources.title_sign_in_to_solr
import org.apache.solr.ui.views.components.SolrCard
import org.apache.solr.ui.views.components.SolrTextButton
import org.jetbrains.compose.resources.painterResource
import org.jetbrains.compose.resources.stringResource

/**
 * The user authentication content is the composable that will check and display
 * the available authentication options to the user.
 */
@Composable
fun UserAuthenticationContent(
    component: AuthenticationComponent,
    modifier: Modifier = Modifier,
) = Row(
    modifier = modifier,
    horizontalArrangement = Arrangement.spacedBy(16.dp),
    verticalAlignment = Alignment.CenterVertically,
) {
    val model by component.model.collectAsState()

    Image(
        modifier = Modifier.weight(1f)
            .align(Alignment.Bottom)
            .fillMaxWidth()
            .aspectRatio(1f)
            .scale(1.5f),
        alpha = .3f,
        painter = painterResource(Res.drawable.solr_sun),
        contentDescription = stringResource(Res.string.cd_solr_logo),
    )

    Column(modifier = Modifier.weight(1f).padding(16.dp)) {
        SolrTextButton(
            onClick = component::onAbort,
            contentPadding = ButtonDefaults.TextButtonWithIconContentPadding,
        ) {
            Icon(
                imageVector = Icons.Filled.ChevronLeft,
                contentDescription = stringResource(Res.string.cd_back_navigation),
            )
            Text(text = stringResource(Res.string.action_go_back))
        }

        SolrCard(
            modifier = Modifier.widthIn(min = 512.dp, max = 640.dp),
            verticalArrangement = Arrangement.spacedBy(16.dp),
        ) {
            Text(
                text = stringResource(Res.string.title_sign_in_to_solr),
                style = MaterialTheme.typography.headlineMedium,
            )

            Text(
                text = stringResource(Res.string.desc_solr_instance_with_auth, model.url),
                style = MaterialTheme.typography.bodyMedium,
            )

            val basicAuthState by component.basicAuthSlot.subscribeAsState()

            basicAuthState.child?.let { basicAuth ->
                BasicAuthContent(
                    modifier = Modifier.testTag("basic_auth_content"),
                    component = basicAuth.instance,
                    isAuthenticating = model.isAuthenticating,
                )
            }

            model.error?.let { error ->
                Text(
                    modifier = Modifier.testTag("error_text"),
                    text = stringResource(resource = error),
                    color = MaterialTheme.colorScheme.error,
                    style = MaterialTheme.typography.bodyMedium,
                )
            }
        }
    }
}
