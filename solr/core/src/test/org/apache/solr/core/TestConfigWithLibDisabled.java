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
package org.apache.solr.core;

import static org.apache.solr.core.SolrConfig.LIB_ENABLED_SYSPROP;
import static org.hamcrest.core.StringContains.containsString;

import java.io.IOException;
import org.apache.solr.SolrTestCaseJ4;
import org.junit.BeforeClass;
import org.junit.Test;

// TODO - replace 'initCore' usage and merge with TestConfig
/**
 * Unit test verifying that "lib" tags are quietly ignored when not explicitly enabled.
 *
 * <p>Based on code from {@link TestConfig}.
 */
public class TestConfigWithLibDisabled extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    System.setProperty(LIB_ENABLED_SYSPROP, "false"); // <lib> tags disabled!
    initCore("solrconfig-test-misc.xml", "schema-reversed.xml");
  }

  // solrconfig-test-misc has lib tags referencing various files
  // This test ensures that none of those files are loadable when
  // <lib> tags are disabled
  @Test
  public void testLibFilesShouldntBeVisible() throws IOException {
    SolrResourceLoader loader = h.getCore().getResourceLoader();
    String[] filesReferencedByLib =
        new String[] {
          "empty-file-a1.txt",
          "empty-file-a2.txt",
          "empty-file-b1.txt",
          "empty-file-b2.txt",
          "empty-file-c1.txt"
        };
    for (String f : filesReferencedByLib) {
      final var e =
          expectThrows(
              SolrResourceNotFoundException.class,
              () -> {
                loader.openResource(f);
              });
      assertThat(e.getMessage(), containsString("Can't find resource"));
      assertThat(e.getMessage(), containsString(f));
    }
  }
}
