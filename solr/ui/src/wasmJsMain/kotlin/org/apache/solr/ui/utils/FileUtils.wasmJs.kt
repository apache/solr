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

package org.apache.solr.ui.utils

import kotlin.coroutines.resume
import kotlinx.browser.document
import kotlinx.coroutines.suspendCancellableCoroutine
import org.apache.solr.ui.domain.PickedFile
import org.khronos.webgl.ArrayBuffer
import org.khronos.webgl.Uint8Array
import org.khronos.webgl.get
import org.w3c.dom.HTMLInputElement
import org.w3c.dom.asList
import org.w3c.dom.events.Event
import org.w3c.files.File
import org.w3c.files.FileReader

actual suspend fun pickFile(
    extensions: List<String>,
): PickedFile? = suspendCancellableCoroutine { cont ->
    val input = (document.createElement("input") as HTMLInputElement).apply {
        type = "file"
        style.display = "none"
        if (extensions.isNotEmpty()) {
            // Accept expects things like ".zip,.json" or MIME types.
            accept = extensions.joinToString(",") { ".${it.trimStart('.')}" }
        }
    }

    fun cleanup() {
        input.onchange = null
        input.remove()
    }

    input.onchange = onchange@{
        val chosen: File? = input.files?.asList()?.firstOrNull()
        if (chosen == null) {
            cleanup()
            cont.resume(null)
            return@onchange
        }

        val reader = FileReader()
        reader.onload = onload@{ _: Event ->
            val buffer = reader.result as? ArrayBuffer
            if (buffer == null) {
                cleanup()
                cont.resume(null)
                return@onload
            }

            val bytes = Uint8Array(buffer)
            val out = ByteArray(bytes.length) { i ->
                bytes[i].toInt().toByte()
            }

            cleanup()
            cont.resume(
                PickedFile(
                    name = chosen.name,
                    bytes = out,
                    extension = chosen.type.takeIf { it.isNotBlank() },
                ),
            )
        }

        reader.onerror = { _: Event ->
            cleanup()
            cont.resume(null)
        }

        reader.readAsArrayBuffer(chosen)
        cont.invokeOnCancellation {
            try {
                reader.abort()
            } catch (_: Throwable) {
                // Ignore: abort() may throw depending on state/platform bindings.
            }
            cleanup()
        }
    }

    document.body?.appendChild(input)
    input.click()
}
