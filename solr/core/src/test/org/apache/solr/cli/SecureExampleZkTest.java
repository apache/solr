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
import org.apache.solr.common.cloud.VMParamsZkCredentialsInjector;

/** Tests persistence and validation of the secure example's ZooKeeper credentials. */
public class SecureExampleZkTest extends SolrTestCase {
  public void testCredentialsRoundTrip() throws Exception {
    var home = createTempDir();
    var secureZk = new SecureExampleZk(home);
    String password = " spaces : = \\ café ";
    secureZk.create(password, "readonly");
    var include = home.resolve("solr.in.sh");
    Files.writeString(include, "SOLR_OPTS='-Xmx512m'\n");
    Files.writeString(home.resolve("solr.xml"), "<solr><solrcloud></solrcloud></solr>");
    secureZk.configure(include, 8983);
    var credentials = new VMParamsZkCredentialsInjector().getZkCredentials();
    assertEquals(password, credentials.getFirst().getPassword());
    assertEquals("readonly", credentials.get(1).getPassword());
    if (home.getFileSystem().supportedFileAttributeViews().contains("posix")) {
      assertEquals(
          PosixFilePermissions.fromString("rw-------"),
          Files.getPosixFilePermissions(secureZk.credentialsFile()));
    }
    String includeContents = Files.readString(include);
    secureZk.configure(include, 8983);
    assertEquals(includeContents, Files.readString(include));
    assertTrue(includeContents.contains("SOLR_OPTS='-Xmx512m'"));
  }

  public void testExistingUnsecuredData() throws Exception {
    var home = createTempDir();
    Files.createDirectory(home.resolve("zoo_data"));
    var secureZk = new SecureExampleZk(home);
    var error =
        expectThrows(IllegalStateException.class, () -> secureZk.create("admin", "readonly"));
    assertTrue(error.getMessage().contains("migrate its ACLs while Solr is stopped"));
    assertFalse(Files.exists(secureZk.credentialsFile()));
  }

  public void testMissingCredentialsFailClosed() throws Exception {
    var home = createTempDir();
    var secureZk = new SecureExampleZk(home);
    Files.writeString(secureZk.credentialsFile(), "zkDigestUsername=zk-admin\n");
    var include = home.resolve("solr.in.sh");
    Files.writeString(include, "");
    expectThrows(IllegalStateException.class, () -> secureZk.configure(include, 8983));
    assertEquals("", Files.readString(include));
  }
}
