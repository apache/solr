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

package org.apache.solr.composeui.ui.theme

import androidx.compose.runtime.Composable
import androidx.compose.ui.text.font.FontFamily
import org.apache.solr.compose_ui.generated.resources.Res
import org.apache.solr.compose_ui.generated.resources.firacode_variable
import org.apache.solr.compose_ui.generated.resources.raleway_variable
import org.jetbrains.compose.resources.Font

/**
 * Fonts object that holds references to custom fonts that have been added.
 */
object Fonts {

    @Composable
    fun firacode() = FontFamily(Font(Res.font.firacode_variable))

    @Composable
    fun raleway() = FontFamily(Font(Res.font.raleway_variable))
}
