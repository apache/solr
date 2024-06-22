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
package org.apache.solr.client.solrj.request;

import java.util.Arrays;
import org.apache.solr.SolrTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.junit.Test;

public class TestUpdateRequest extends SolrTestCase {

  @Test
  public void testCannotAddNullSolrInputDocument() {
    UpdateRequest req = new UpdateRequest();

    NullPointerException thrown =
        assertThrows(NullPointerException.class, () -> req.add((SolrInputDocument) null));
    assertEquals("Cannot add a null SolrInputDocument", thrown.getMessage());
  }

  @Test
  public void testCannotAddNullDocumentWithOverwrite() {
    UpdateRequest req = new UpdateRequest();

    NullPointerException thrown =
        assertThrows(NullPointerException.class, () -> req.add(null, true));
    assertEquals("Cannot add a null SolrInputDocument", thrown.getMessage());
  }

  @Test
  public void testCannotAddNullDocumentWithCommitWithin() {
    UpdateRequest req = new UpdateRequest();

    NullPointerException thrown = assertThrows(NullPointerException.class, () -> req.add(null, 1));
    assertEquals("Cannot add a null SolrInputDocument", thrown.getMessage());
  }

  @Test
  public void testCannotAddNullDocumentWithParameters() {
    UpdateRequest req = new UpdateRequest();

    NullPointerException thrown =
        assertThrows(NullPointerException.class, () -> req.add(null, 1, true));
    assertEquals("Cannot add a null SolrInputDocument", thrown.getMessage());
  }

  @Test
  public void testCannotAddNullDocumentAsPartOfList() {
    UpdateRequest req = new UpdateRequest();

    NullPointerException thrown =
        assertThrows(
            NullPointerException.class,
            () -> req.add(Arrays.asList(new SolrInputDocument(), new SolrInputDocument(), null)));
    assertEquals("Cannot add a null SolrInputDocument", thrown.getMessage());
  }
}
