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

package org.apache.solr.cli;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.solr.SolrTestCase;
import org.apache.solr.common.util.EnvUtils;
import org.junit.Test;

/** Tests the include-file editing shared by authentication and SSL tools. */
public class IncludeFileEditorTest extends SolrTestCase {
  @Test
  public void testShellUpdatesAreIdempotent() {
    String original =
        "# Keep this comment\r\nexport SOLR_AUTH_TYPE=old\r\nSOLR_AUTH_TYPE=duplicate\r\nSOLR_SSL_ENABLED=true\r\n";
    var settings = Map.of("SOLR_AUTH_TYPE", "basic");
    String updated =
        IncludeFileEditor.update(
            original, settings, settings.keySet(), IncludeFileEditor.Format.SHELL);
    assertEquals(
        "# Keep this comment\r\nSOLR_SSL_ENABLED=true\r\nSOLR_AUTH_TYPE='basic'\r\n", updated);
    assertEquals(
        updated,
        IncludeFileEditor.update(
            updated, settings, settings.keySet(), IncludeFileEditor.Format.SHELL));
  }

  @Test
  public void testWindowsAssignments() {
    String original =
        "REM Keep this comment\r\nset SOLR_AUTH_TYPE=old\r\nSET \"solr_auth_type=duplicate\"\r\nset SOLR_AUTHENTICATION_CLIENT_BUILDER=old\r\nset SOLR_SSL_ENABLED=true\r\n";
    var settings = new LinkedHashMap<String, String>();
    settings.put("SOLR_AUTH_TYPE", "basic");
    settings.put(
        "SOLR_AUTHENTICATION_OPTS",
        "\"-Dsolr.httpclient.config=C:\\Program Files\\Solr\\basicAuth.conf\"");
    var replaced =
        Set.of("SOLR_AUTH_TYPE", "SOLR_AUTHENTICATION_OPTS", "SOLR_AUTHENTICATION_CLIENT_BUILDER");
    String updated =
        IncludeFileEditor.update(original, settings, replaced, IncludeFileEditor.Format.WINDOWS);
    assertEquals(
        "REM Keep this comment\r\nset SOLR_SSL_ENABLED=true\r\nset SOLR_AUTH_TYPE=basic\r\nset SOLR_AUTHENTICATION_OPTS=\"-Dsolr.httpclient.config=C:\\Program Files\\Solr\\basicAuth.conf\"\r\n",
        updated);
    assertEquals(
        updated,
        IncludeFileEditor.update(updated, settings, replaced, IncludeFileEditor.Format.WINDOWS));
  }

  @Test
  public void testDisablingSettings() {
    for (var format : IncludeFileEditor.Format.values()) {
      String assignment =
          format == IncludeFileEditor.Format.SHELL
              ? "export SOLR_AUTH_TYPE='basic'"
              : "set \"SOLR_AUTH_TYPE=basic\"";
      String original = assignment + "\r\n# unchanged\r\n";
      String updated = IncludeFileEditor.disable(original, Set.of("SOLR_AUTH_TYPE"), format);
      String comment = format == IncludeFileEditor.Format.SHELL ? "# " : "REM ";
      assertEquals(comment + assignment + "\r\n# unchanged\r\n", updated);
      assertEquals(updated, IncludeFileEditor.disable(updated, Set.of("SOLR_AUTH_TYPE"), format));
      assertEquals(
          "# untouched",
          IncludeFileEditor.disable("# untouched", Set.of("SOLR_AUTH_TYPE"), format));
    }
  }

  @Test
  public void testShellQuoting() throws Exception {
    assumeTrue(
        "External programs require -Ptests.useSecurityManager=false",
        EnvUtils.getProperty("java.security.manager") == null);
    assumeFalse("Requires bash", EnvUtils.getProperty("os.name").startsWith("Windows"));
    Path dir = createTempDir();
    Path include = dir.resolve("solr.in.sh");
    String value = "value' $HOME `false` $(false)";
    var settings = Map.of("SOLR_AUTHENTICATION_OPTS", value);
    Files.writeString(
        include,
        IncludeFileEditor.update("", settings, settings.keySet(), IncludeFileEditor.Format.SHELL));
    ProcessBuilder builder =
        new ProcessBuilder(
            "bash",
            "-c",
            "source \"$1\"; test \"$SOLR_AUTHENTICATION_OPTS\" = \"$EXPECTED_SHARED_VALUE\"",
            "test",
            include.toString());
    builder.environment().put("EXPECTED_SHARED_VALUE", value);
    Process shell = builder.start();
    try {
      assertTrue(shell.waitFor(10, TimeUnit.SECONDS));
      assertEquals(0, shell.exitValue());
    } finally {
      if (shell.isAlive()) shell.destroyForcibly().waitFor();
    }
  }

  @Test
  public void testNewFilePermissions() throws Exception {
    Path file = createTempDir().resolve("basicAuth.conf");
    IncludeFileEditor.write(file, "credentials".getBytes(StandardCharsets.UTF_8));
    assertEquals("credentials", Files.readString(file));
    if (file.getFileSystem().supportedFileAttributeViews().contains("posix")) {
      assertEquals(
          PosixFilePermissions.fromString("rw-------"), Files.getPosixFilePermissions(file));
    }
  }

  @Test
  public void testServiceIncludePermissionsArePreserved() throws Exception {
    Path include = createTempDir().resolve("solr.in.sh");
    assumeTrue(
        "Requires POSIX filesystem",
        include.getFileSystem().supportedFileAttributeViews().contains("posix"));
    Files.writeString(include, "original");
    Files.setPosixFilePermissions(include, PosixFilePermissions.fromString("rw-r-----"));
    var original = Files.readAttributes(include, PosixFileAttributes.class);

    IncludeFileEditor.write(include, "updated".getBytes(StandardCharsets.UTF_8));

    var updated = Files.readAttributes(include, PosixFileAttributes.class);
    assertEquals("updated", Files.readString(include));
    assertEquals(original.owner(), updated.owner());
    assertEquals(original.group(), updated.group());
    assertEquals(original.permissions(), updated.permissions());
    assertEquals(original.fileKey(), updated.fileKey());
  }

  @Test
  public void testSymlinkIsPreserved() throws Exception {
    assumeTrue(
        "Symbolic links require -Ptests.useSecurityManager=false",
        EnvUtils.getProperty("java.security.manager") == null);
    Path dir = createTempDir();
    assumeTrue(
        "Requires POSIX filesystem",
        dir.getFileSystem().supportedFileAttributeViews().contains("posix"));
    Path target = dir.resolve("actual.in.sh");
    Files.writeString(target, "original");
    Path link = Files.createSymbolicLink(dir.resolve("solr.in.sh"), target);
    IncludeFileEditor.write(link, "updated".getBytes(StandardCharsets.UTF_8));
    assertTrue(Files.isSymbolicLink(link));
    assertEquals("updated", Files.readString(target));
  }
}
