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

package org.apache.solr.handler.admin;

import static org.apache.solr.handler.admin.SecurityConfHandler.SecurityConfig;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.lucene.tests.util.LuceneTestCase.SuppressFileSystems;
import org.apache.solr.SolrTestCase;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.security.RuleBasedAuthorizationPlugin;

/**
 * Reads security.json while edits replace it. The reader keeps the file open while the writer
 * renames a new file over it. The Windows-like test filesystem cannot track that case, so this
 * class runs without it.
 */
@SuppressFileSystems({"ExtrasFS", "WindowsFS"})
public class SecurityConfHandlerReaderTest extends SolrTestCase {

  /** A reader of security.json never sees a partial document while edits are written. */
  public void testReaderNeverSeesPartialSecurityJsonDuringEdits() throws Exception {
    Path solrHome = createTempDir("securityConfHandlerLocalReaders");
    assumeTrue(
        "atomic replacement needs POSIX file attributes",
        solrHome.getFileSystem().supportedFileAttributeViews().contains("posix"));
    Path securityJson = solrHome.resolve("security.json");
    SecurityConfHandlerLocal handler = new SecurityConfHandlerLocal(mockCoreContainer(solrHome));
    Map<String, Object> small = shortSecurityData("x");
    Map<String, Object> large = largeSecurityData();
    byte[] smallBytes = Utils.toJSON(small);
    byte[] largeBytes = Utils.toJSON(large);
    Files.write(securityJson, smallBytes);
    AtomicReference<Throwable> writerErr = new AtomicReference<>();
    AtomicBoolean writing = new AtomicBoolean(true);
    Thread writer =
        new Thread(
            () -> {
              try {
                for (int i = 0; i < 100; i++) {
                  handler.persistConf(new SecurityConfig().setData(i % 2 == 0 ? large : small));
                }
              } catch (Throwable t) {
                writerErr.set(t);
              } finally {
                writing.set(false);
              }
            });
    writer.start();
    int reads = 0;
    try {
      while (writing.get()) {
        byte[] got;
        try {
          got = Files.readAllBytes(securityJson);
        } catch (IOException e) {
          // NFS reports a stale handle when the file is replaced during a read
          continue;
        }
        assertTrue(
            "reader saw a partial security.json of " + got.length + " bytes",
            Arrays.equals(got, smallBytes) || Arrays.equals(got, largeBytes));
        reads++;
      }
    } finally {
      writer.join(TimeUnit.SECONDS.toMillis(60));
    }
    assertTrue("reader made no reads while the writer ran", reads > 0);
    assertNull("writer failed: " + writerErr.get(), writerErr.get());
  }

  private static Map<String, Object> largeSecurityData() {
    Map<String, Object> creds = new LinkedHashMap<>();
    creds.put("large", "L".repeat(1_000_000));
    Map<String, Object> authc = new LinkedHashMap<>();
    authc.put("class", "solr.BasicAuthPlugin");
    authc.put("credentials", creds);
    Map<String, Object> data = new LinkedHashMap<>();
    data.put("authentication", authc);
    return data;
  }

  private static Map<String, Object> shortSecurityData(CharSequence credValue) {
    Map<String, Object> creds = new LinkedHashMap<>();
    creds.put("u", credValue);
    Map<String, Object> authc = new LinkedHashMap<>();
    authc.put("class", "solr.BasicAuthPlugin");
    authc.put("credentials", creds);
    Map<String, Object> data = new LinkedHashMap<>();
    data.put("authentication", authc);
    return data;
  }

  private static CoreContainer mockCoreContainer(Path solrHome) {
    CoreContainer cc = mock(CoreContainer.class);
    when(cc.getSolrHome()).thenReturn(solrHome);
    when(cc.getAuthorizationPlugin()).thenReturn(new RuleBasedAuthorizationPlugin());
    return cc;
  }
}
