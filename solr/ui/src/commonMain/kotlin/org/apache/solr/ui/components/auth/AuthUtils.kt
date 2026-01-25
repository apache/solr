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

package org.apache.solr.ui.components.auth

import kotlin.io.encoding.Base64
import kotlin.io.encoding.Base64.PaddingOption
import kotlin.random.Random
import okio.ByteString
import okio.ByteString.Companion.encodeUtf8

/**
 * Characters allowed by RFC 7636 for the code verifier
 */
private const val CODE_VERIFIER_CHARSET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~"

private const val CODE_VERIFIER_LENGTH = 128

private const val STATE_CHARSET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"

private const val STATE_LENGTH = 32

internal fun generateCodeVerifier(): String = buildString {
    repeat(CODE_VERIFIER_LENGTH) {
        append(CODE_VERIFIER_CHARSET[Random.nextInt(CODE_VERIFIER_CHARSET.length)])
    }
}

internal fun generateCodeChallenge(verifier: String): String {
    val sha256Bytes: ByteString = verifier.encodeUtf8().sha256()
    return Base64.UrlSafe.withPadding(PaddingOption.ABSENT).encode(sha256Bytes.toByteArray())
}

internal fun generateOAuthState(): String = buildString {
    repeat(STATE_LENGTH) {
        append(STATE_CHARSET[Random.nextInt(STATE_CHARSET.length)])
    }
}

/**
 * Retrieve the redirect URI for the OAuth flow.
 *
 * This URI is platform-specific, as it uses different redirect URIs on different platforms.
 */
internal expect fun getRedirectUri(): String
