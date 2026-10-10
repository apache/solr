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

package org.apache.lucene.gradle;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.file.DirectoryProperty;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.tasks.CacheableTask;
import org.gradle.api.tasks.InputDirectory;
import org.gradle.api.tasks.InputFile;
import org.gradle.api.tasks.OutputDirectory;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.TaskAction;

/** Renders CHANGELOG.md as Changes.html and copies the stylesheets that the page uses. */
@CacheableTask
public abstract class ChangesToHtmlTask extends DefaultTask {

  @InputFile
  @PathSensitive(PathSensitivity.RELATIVE)
  public abstract RegularFileProperty getChangesFile();

  @InputDirectory
  @PathSensitive(PathSensitivity.RELATIVE)
  public abstract DirectoryProperty getSiteDir();

  @OutputDirectory
  public abstract DirectoryProperty getTargetDir();

  @TaskAction
  public void convert() throws IOException {
    Path changes = getChangesFile().get().getAsFile().toPath();
    if (!Files.exists(changes)) {
      throw new GradleException("Changes file " + changes + " not found.");
    }

    Path target = Files.createDirectories(getTargetDir().get().getAsFile().toPath());
    ChangesToHtml.write(changes, target.resolve("Changes.html"));

    try (DirectoryStream<Path> stylesheets =
        Files.newDirectoryStream(getSiteDir().get().getAsFile().toPath(), "*.css")) {
      for (Path stylesheet : stylesheets) {
        Files.copy(
            stylesheet,
            target.resolve(stylesheet.getFileName()),
            StandardCopyOption.REPLACE_EXISTING);
      }
    }
  }
}
