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
package org.apache.solr.search.grouping.distributed.command;

import org.apache.lucene.search.Sort;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.schema.SchemaField;
import org.junit.BeforeClass;
import org.junit.Test;

public class SearchGroupsFieldCommandTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
  }

  @Test
  public void testMultiValuedFieldIsRejected() {
    SchemaField cat = h.getCore().getLatestSchema().getField("cat");
    assertTrue(cat.multiValued());

    SolrException e =
        expectThrows(
            SolrException.class,
            () ->
                new SearchGroupsFieldCommand.Builder()
                    .setField(cat)
                    .setGroupSort(Sort.RELEVANCE)
                    .setTopNGroups(10)
                    .build());
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, e.code());
    assertTrue(e.getMessage(), e.getMessage().contains("'cat' is multiValued"));
  }

  @Test
  public void testSingleValuedFieldIsAccepted() {
    SchemaField id = h.getCore().getLatestSchema().getField("id");
    assertFalse(id.multiValued());
    new SearchGroupsFieldCommand.Builder()
        .setField(id)
        .setGroupSort(Sort.RELEVANCE)
        .setTopNGroups(10)
        .build();
  }
}
