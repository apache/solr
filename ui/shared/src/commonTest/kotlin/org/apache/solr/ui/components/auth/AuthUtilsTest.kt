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

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class AuthUtilsTest {

    @Test
    fun `WHEN generate code verifier THEN length is CODE_VERIFIER_LENGTH`() {
        val codeVerifier = generateCodeVerifier()
        assertEquals(
            expected = CODE_VERIFIER_LENGTH,
            actual = codeVerifier.length,
            message = "code verifier length should be $CODE_VERIFIER_LENGTH characters",
        )
    }

    @Test
    fun `WHEN generate code verifier THEN used characters in CODE_VERIFIER_CHARSET`() {
        val codeVerifier = generateCodeVerifier()
        codeVerifier.all {
            assertTrue(
                actual = it in CODE_VERIFIER_CHARSET,
                message = """Code verifier character "$it" should be only allowed characters from RFC 7636""",
            )
            true
        }
    }

    @Test
    fun `WHEN generate OAuth state THEN length is STATE_LENGTH`() {
        val state = generateOAuthState()
        assertEquals(
            expected = STATE_LENGTH,
            actual = state.length,
            message = "state length should be $STATE_LENGTH characters",
        )
    }

    @Test
    fun `WHEN generate OAuth state THEN used characters in STATE_CHARSET`() {
        val state = generateOAuthState()
        state.all {
            assertTrue(
                actual = it in STATE_CHARSET,
                message = """OAuth state character "$it" should be one of $STATE_CHARSET""",
            )
            true
        }
    }

    @Test
    fun `GIVEN valid code verifier WHEN generate code challenge THEN all output characters are URL safe`() {
        // Use a fix code verifier to include all characters, instead of randomly generating one
        val codeVerifier =
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
        val actualCodeChallenge = generateCodeChallenge(codeVerifier)
        val urlSafeCharacters = CODE_VERIFIER_CHARSET

        actualCodeChallenge.all {
            assertTrue(
                actual = it in urlSafeCharacters,
                message = """Code challenge character "$it" is not URL-safe""",
            )
            true
        }
    }

    @Test
    fun `GIVEN valid code verifier WHEN generate hashed code challenge THEN all characters are URL safe`() {
        // Use a fix code verifier to include all characters, instead of randomly generating one
        val codeVerifier =
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
        val expectedCodeChallenge = "Gn88msbRKQ0wmy6Kms0RzrR4ZXFo3OGDewwvI9C7qZg"
        val actualCodeChallenge = generateCodeChallenge(codeVerifier)
        assertEquals(
            expected = expectedCodeChallenge,
            actual = actualCodeChallenge,
            message = "code challenge hash ",
        )
        assertFalse(
            actual = actualCodeChallenge.contains("="),
            message = "Code challenge should not contain padding characters",
        )
    }

    @Test
    fun `GIVEN valid fixed code verifier WHEN generate hashed code challenge THEN no paddings included`() {
        // Use a fix code verifier to include all characters, instead of randomly generating one
        val codeVerifier =
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
        val actualCodeChallenge = generateCodeChallenge(codeVerifier)

        // With padding the code verifier would generate Gn88msbRKQ0wmy6Kms0RzrR4ZXFo3OGDewwvI9C7qZg=
        // with a padding character at the end.
        assertFalse(
            actual = actualCodeChallenge.contains("="),
            message = "Code challenge should not contain padding characters",
        )
    }
}
