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

package org.apache.solr.ui.components.configsets.domain

import org.apache.solr.ui.components.configsets.repository.ConfigsetsRepository
import org.apache.solr.ui.utils.MAX_CONFIGSET_NAME_LENGTH
import org.apache.solr.ui.utils.configsetNameRegex

internal class DefaultCreateConfigsetUseCase(
    private val repository: ConfigsetsRepository,
) : CreateConfigsetUseCase {
    override suspend fun invoke(
        configsetName: String,
        baseConfigset: String?,
    ): CreateConfigsetResult {
        if (!configsetName.matches(configsetNameRegex)) {
            return CreateConfigsetResult.ValidationFailure(
                error = CreateConfigsetResult.Error.ConfigsetNameContainsInvalidCharacters
            )
        }

        if (configsetName.length > MAX_CONFIGSET_NAME_LENGTH) {
            return CreateConfigsetResult.ValidationFailure(
                error = CreateConfigsetResult.Error.ConfigsetNameTooLong
            )
        }

        // TODO v2 API requires baseConfigSet to be set,
        //  remove fallback to _default once it is truly optional
        repository.createConfigset(configsetName, baseConfigset ?: "_default")
            .onSuccess {
                return CreateConfigsetResult.Success(configset = it)
            }
            .onFailure { return CreateConfigsetResult.UnexpectedFailure(cause = it) }
        return CreateConfigsetResult.UnexpectedFailure(cause = Error("Unknown error"))
    }
}
