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

package org.apache.solr.ui.errors

import io.ktor.http.Url
import org.apache.solr.ui.domain.AuthMethod

/**
 * Exception that is thrown for unauthorized access to the API.
 *
 * This is usually used when a response has HTTP status code 401.
 *
 * @property url The URL that threw the unauthorized response.
 * @property methods The authentication methods the unauthorized response contains.
 * This can be used for further redirecting and authenticating the user.
 */
class UnauthorizedException(
    val url: Url? = null,
    val methods: List<AuthMethod> = emptyList(),
    message: String? = null,
) : Exception(message)
