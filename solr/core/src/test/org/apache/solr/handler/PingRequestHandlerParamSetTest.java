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
package org.apache.solr.handler;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import org.apache.commons.io.file.PathUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.junit.BeforeClass;

/**
 * The implicit {@code /admin/ping} handler declares {@code useParams=_ADMIN_PING}, so the handler
 * it delegates to may also be configured through a paramset in {@code params.json}.
 */
public class PingRequestHandlerParamSetTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    Path solrHome = createTempDir();
    PathUtils.copyDirectory(TEST_HOME(), solrHome, StandardCopyOption.COPY_ATTRIBUTES);
    Files.writeString(
        solrHome.resolve("collection1").resolve("conf").resolve("params.json"),
        "{\"params\":{\"_ADMIN_PING\":{\"_invariants_\":{\"qt\":\"/nosuchhandler\"}}}}");
    initCore("solrconfig.xml", "schema.xml", solrHome);
  }

  public void testDelegateHandlerFromParamSet() {
    SolrException se =
        expectThrows(SolrException.class, () -> h.query("/admin/ping", req("qt", "/select")));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, se.code());
    assertTrue(se.getMessage(), se.getMessage().contains("/nosuchhandler"));
  }
}
