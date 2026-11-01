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

import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermissions;
import org.apache.solr.SolrTestCase;

/** Checks that customized HTTP credentials survive an example restart. */
public class SecureExampleCredentialsTest extends SolrTestCase {
  public void testSavedPasswords() throws Exception {
    var home = createTempDir();
    var accounts = new SecureExampleCredentials(home);
    assertFalse(accounts.exists());
    assertEquals("SolrRocks", accounts.password("admin"));
    accounts.setPassword("index", " spaces : = \\ café ");
    accounts.setAdministrator("rob:Admin:Password");
    accounts.save();
    var restored = new SecureExampleCredentials(home);
    assertEquals("rob", restored.adminUsername());
    assertEquals("Admin:Password", restored.password("admin"));
    assertEquals(accounts.password("index"), restored.password("index"));
    if (home.getFileSystem().supportedFileAttributeViews().contains("posix")) {
      assertEquals(
          PosixFilePermissions.fromString("rw-------"),
          Files.getPosixFilePermissions(accounts.file()));
    }
  }

  public void testInvalidAdministrator() throws Exception {
    var accounts = new SecureExampleCredentials(createTempDir());
    expectThrows(IllegalArgumentException.class, () -> accounts.setAdministrator("no-password"));
    expectThrows(IllegalArgumentException.class, () -> accounts.setAdministrator("index:secret"));
    assertEquals("solr", accounts.adminUsername());
  }

  public void testIncompleteCredentials() throws Exception {
    var home = createTempDir();
    Files.writeString(home.resolve("http-credentials.properties"), "admin.username=solr\n");
    expectThrows(IllegalStateException.class, () -> new SecureExampleCredentials(home));
  }
}
