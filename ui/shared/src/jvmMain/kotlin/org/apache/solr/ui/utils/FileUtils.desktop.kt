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

import java.awt.FileDialog
import java.awt.Frame
import java.io.File
import javax.swing.SwingUtilities
import kotlin.coroutines.resume
import kotlinx.coroutines.suspendCancellableCoroutine
import org.apache.solr.ui.domain.PickedFile

actual suspend fun pickFile(extensions: List<String>): PickedFile? {
    val (dir, fileName) = suspendCancellableCoroutine { cont ->
        SwingUtilities.invokeLater {
            val dialog = FileDialog(null as Frame?, "Choose a file", FileDialog.LOAD).apply {
                isMultipleMode = false
                // Note: FileDialog filtering is platform-dependent; setFilenameFilter is best-effort.
                if (extensions.isNotEmpty()) {
                    setFilenameFilter { _, name ->
                        val lower = name.lowercase()
                        extensions.any { ext -> lower.endsWith(".${ext.lowercase()}") }
                    }
                }
                isVisible = true
            }
            cont.resume(dialog.directory to dialog.file)
            dialog.dispose()
        }
    }

    if (dir == null || fileName == null) return null

    val f = File(dir, fileName)
    return PickedFile(
        name = f.name,
        bytes = f.readBytes(),
        extension = f.extension,
    )
}
