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

import kotlin.test.Ignore
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlinx.browser.window

class AuthUtilsTestJs {

    @Test
    fun `WHEN generate redirect URI with defaults THEN redirect URI appends callback to the end`() {
        val redirectUri = getRedirectUri()

        assertContains(
            charSequence = redirectUri,
            other = "callback",
            message = """The path "callback" should be in the generated redirect URI""",
        )

        assertTrue(
            actual = redirectUri.endsWith("/callback"),
            message = """The generated redirect URI should end with "/callback".""",
        )
    }

    @Test
    fun `WHEN generate redirect URI with defaults THEN no double slash exists`() {
        val redirectUri = getRedirectUri()

        assertFalse(
            actual = redirectUri.contains("//"),
            message = """The generated URI "$redirectUri" should contain double slashes.""",
        )
    }

    @Test
    fun `WHEN generate redirect URI THEN redirect URI contains current origin`() {
        val redirectUri = getRedirectUri()

        assertContains(
            charSequence = redirectUri,
            other = window.location.origin,
            message = "Origin should be part of the generated redirect URI",
        )
    }

    /**
     * Future scenario for allowing overriding base path.
     */
    @Test
    @Ignore
    fun `WHEN generate redirect URI with custom path THEN only custom path included`() {
        // val redirectUri = getRedirectUri("/custom/path")

        // assertContains(
        //     charSequence = redirectUri,
        //     other = "callback",
        //     message = """The path "callback" should be in the generated redirect URI""",
        // )
    }
}
