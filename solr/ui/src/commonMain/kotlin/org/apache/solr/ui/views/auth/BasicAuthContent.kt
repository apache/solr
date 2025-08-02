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

import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.material3.LinearProgressIndicator
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.ui.Modifier
import androidx.compose.ui.platform.testTag
import androidx.compose.ui.text.input.PasswordVisualTransformation
import androidx.compose.ui.unit.dp
import org.apache.solr.ui.components.auth.BasicAuthComponent
import org.apache.solr.ui.generated.resources.Res
import org.apache.solr.ui.generated.resources.action_sign_in_with_credentials
import org.apache.solr.ui.generated.resources.authenticating
import org.apache.solr.ui.generated.resources.desc_sign_in_with_credentials_to_realm
import org.apache.solr.ui.generated.resources.label_password
import org.apache.solr.ui.generated.resources.label_username
import org.apache.solr.ui.views.components.SolrButton
import org.jetbrains.compose.resources.stringResource

/**
 * The basic auth content is a form where the user can provide credentials to authenticate and
 * sign in to a Solr instance.
 *
 * @param component The [BasicAuthComponent] that is handling the interactions with this composable.
 * @param modifier Modifier that is applied to the root of this composable.
 * @param isAuthenticating Whether the user is currently being authenticated. This disables the inputs
 * and updates the text shown in the button.
 */
@Composable
fun BasicAuthContent(
    component: BasicAuthComponent,
    modifier: Modifier = Modifier,
    isAuthenticating: Boolean = true,
) = Column(
    modifier = modifier,
    verticalArrangement = Arrangement.spacedBy(16.dp),
) {
    val model by component.model.collectAsState()

    Text(
        text = stringResource(Res.string.desc_sign_in_with_credentials_to_realm, model.realm),
        style = MaterialTheme.typography.bodyMedium,
    )

    OutlinedTextField(
        modifier = Modifier.fillMaxWidth().testTag(tag = "username_input_field"),
        value = model.username,
        singleLine = true,
        isError = model.hasError,
        label = { Text(stringResource(Res.string.label_username)) },
        onValueChange = component::onChangeUsername,
        enabled = !isAuthenticating,
    )

    OutlinedTextField(
        modifier = Modifier.fillMaxWidth().testTag(tag = "password_input_field"),
        value = model.password,
        singleLine = true,
        isError = model.hasError,
        label = { Text(stringResource(Res.string.label_password)) },
        visualTransformation = PasswordVisualTransformation(),
        onValueChange = component::onChangePassword,
        enabled = !isAuthenticating,
    )

    Column {
        SolrButton(
            modifier = Modifier.fillMaxWidth().testTag(tag = "sign_in_button"),
            onClick = component::onAuthenticate,
            enabled = !isAuthenticating,
        ) {
            Text(
                text = if (isAuthenticating) {
                    stringResource(Res.string.authenticating)
                } else {
                    stringResource(Res.string.action_sign_in_with_credentials)
                },
            )
        }
        if (isAuthenticating) {
            LinearProgressIndicator(
                modifier = Modifier.fillMaxWidth().testTag(tag = "loading_indicator"),
            )
        }
    }
}
