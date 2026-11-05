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

import java.util.List;
import java.util.Map;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.util.Utils;
import org.junit.BeforeClass;
import org.junit.Test;

public class ReplicationCommitsV1ResponseTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-leader1.xml", "schema.xml");
  }

  @Test
  public void testV1CommitsFlatNamedListShape() throws Exception {
    assertU(adoc("id", "1"));
    assertU(commit());

    String json =
        h.query("/replication", req("command", "commits", "wt", "json", "json.nl", "flat"));
    assertFirstCommitIsFlatNamedList(json);
  }

  @Test
  public void testV1CommitsDefaultJsonShape() throws Exception {
    assertU(adoc("id", "1"));
    assertU(commit());

    String json = h.query("/replication", req("command", "commits", "wt", "json"));
    assertFirstCommitIsFlatNamedList(json);
  }

  private static void assertFirstCommitIsFlatNamedList(String json) {
    List<?> commits = (List<?>) ((Map<?, ?>) Utils.fromJSONString(json)).get("commits");
    assertFalse("expected at least one commit point", commits.isEmpty());
    Object firstCommit = commits.get(0);
    assertTrue(
        "v1 commits entry must stay a NamedList rendered as a flat name/value array, got: "
            + firstCommit,
        firstCommit instanceof List);
    List<?> entry = (List<?>) firstCommit;
    assertEquals("indexVersion", entry.get(0));
    assertEquals("generation", entry.get(2));
  }
}
