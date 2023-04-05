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

package org.apache.solr.update.processor;

import static org.apache.solr.update.processor.IgnoreLargeDocumentProcessorFactory.ObjectSizeEstimator.estimate;
import static org.hamcrest.Matchers.containsString;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.solr.SolrTestCase;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.util.LogListener;
import org.hamcrest.MatcherAssert;
import org.junit.Test;

public class IgnoreLargeDocumentProcessorFactoryTest extends SolrTestCase {

  @Test
  public void testProcessor() throws IOException {
    NamedList<Object> args = new NamedList<>();
    args.add(IgnoreLargeDocumentProcessorFactory.LIMIT_SIZE_PARAM, 1);

    IgnoreLargeDocumentProcessorFactory factory = new IgnoreLargeDocumentProcessorFactory();
    factory.init(args);

    UpdateRequestProcessor processor = factory.getInstance(null, null, null);
    expectThrows(SolrException.class, () -> processor.processAdd(getUpdate(1024)));

    args = new NamedList<>();
    args.add(IgnoreLargeDocumentProcessorFactory.LIMIT_SIZE_PARAM, 2);
    factory = new IgnoreLargeDocumentProcessorFactory();
    factory.init(args);
    UpdateRequestProcessor requestProcessor = factory.getInstance(null, null, null);
    requestProcessor.processAdd(getUpdate(1024));
  }

  @Test
  public void testProcessorInPermissiveMode() throws IOException {
    NamedList<Object> args = new NamedList<>();
    args.add(IgnoreLargeDocumentProcessorFactory.LIMIT_SIZE_PARAM, 1);
    args.add(IgnoreLargeDocumentProcessorFactory.PERMISSIVE_MODE_PARAM, true);

    IgnoreLargeDocumentProcessorFactory factory = new IgnoreLargeDocumentProcessorFactory();
    factory.init(args);

    UpdateRequestProcessor processor = factory.getInstance(null, null, null);
    try (LogListener listener = LogListener.warn(IgnoreLargeDocumentProcessorFactory.class)) {
      processor.processAdd(getUpdate(1024));

      MatcherAssert.assertThat(
          listener.pollMessage(),
          containsString("Skipping doc because estimated size exceeds limit"));
    }
  }

  public AddUpdateCommand getUpdate(int sizeBytes) {
    SolrInputDocument document = new SolrInputDocument();
    document.addField(new String(new byte[sizeBytes], Charset.defaultCharset()), 1L);
    assertTrue(
        IgnoreLargeDocumentProcessorFactory.ObjectSizeEstimator.estimate(document) > sizeBytes);

    AddUpdateCommand cmd = new AddUpdateCommand(null);
    cmd.solrDoc = document;
    return cmd;
  }

  @Test
  public void testEstimateObjectSize() {
    assertEquals(estimate("abc"), 6);
    assertEquals(estimate("abcdefgh"), 16);
    List<String> keys = List.of("int", "long", "double", "float", "str");
    assertEquals(estimate(keys), 42);
    List<Object> values = List.of(12, 5L, 12.0, 5.0, "duck");
    assertEquals(estimate(values), 8);

    Map<String, Object> map = new HashMap<>();
    map.put("int", 12);
    map.put("long", 5L);
    map.put("double", 12.0);
    map.put("float", 5.0f);
    map.put("str", "duck");
    assertEquals(estimate(map), 50);

    SolrInputDocument document = new SolrInputDocument();
    for (Map.Entry<String, Object> entry : map.entrySet()) {
      document.addField(entry.getKey(), entry.getValue());
    }
    assertEquals(
        IgnoreLargeDocumentProcessorFactory.ObjectSizeEstimator.estimate(document), estimate(map));

    SolrInputDocument childDocument = new SolrInputDocument();
    for (Map.Entry<String, Object> entry : map.entrySet()) {
      childDocument.addField(entry.getKey(), entry.getValue());
    }
    document.addChildDocument(childDocument);
    assertEquals(
        IgnoreLargeDocumentProcessorFactory.ObjectSizeEstimator.estimate(document),
        estimate(map) * 2);
  }

  @Test
  public void testEstimateObjectSizeWithSingleChild() {
    assertEquals(estimate("abc"), 6);
    assertEquals(estimate("abcdefgh"), 16);
    List<String> keys = List.of("int", "long", "double", "float", "str");
    assertEquals(estimate(keys), 42);
    List<Object> values = List.of(12, 5L, 12.0, 5.0, "duck");
    assertEquals(estimate(values), 8);
    final String childDocKey = "testChildDoc";

    Map<String, Object> mapWChild = new HashMap<>();
    mapWChild.put("int", 12);
    mapWChild.put("long", 5L);
    mapWChild.put("double", 12.0);
    mapWChild.put("float", 5.0f);
    mapWChild.put("str", "duck");
    assertEquals(estimate(mapWChild), 50);
    Map<String, Object> childMap = new HashMap<>(mapWChild);

    SolrInputDocument document = new SolrInputDocument();
    for (Map.Entry<String, Object> entry : mapWChild.entrySet()) {
      document.addField(entry.getKey(), entry.getValue());
    }
    assertEquals(
        IgnoreLargeDocumentProcessorFactory.ObjectSizeEstimator.estimate(document),
        estimate(mapWChild));

    SolrInputDocument childDocument = new SolrInputDocument();
    for (Map.Entry<String, Object> entry : mapWChild.entrySet()) {
      childDocument.addField(entry.getKey(), entry.getValue());
    }
    document.addField(childDocKey, childDocument);
    mapWChild.put(childDocKey, childMap);
    assertEquals(
        IgnoreLargeDocumentProcessorFactory.ObjectSizeEstimator.estimate(document),
        estimate(childMap) * 2 + estimate(childDocKey));
    assertEquals(
        IgnoreLargeDocumentProcessorFactory.ObjectSizeEstimator.estimate(document),
        estimate(mapWChild));
  }

  @Test
  public void testEstimateObjectSizeWithChildList() {
    assertEquals(estimate("abc"), 6);
    assertEquals(estimate("abcdefgh"), 16);
    List<String> keys = List.of("int", "long", "double", "float", "str");
    assertEquals(estimate(keys), 42);
    List<Object> values = List.of(12, 5L, 12.0, 5.0, "duck");
    assertEquals(estimate(values), 8);
    final String childDocKey = "testChildDoc";

    Map<String, Object> mapWChild = new HashMap<>();
    mapWChild.put("int", 12);
    mapWChild.put("long", 5L);
    mapWChild.put("double", 12.0);
    mapWChild.put("float", 5.0f);
    mapWChild.put("str", "duck");
    assertEquals(estimate(mapWChild), 50);
    Map<String, Object> childMap = new HashMap<>(mapWChild);

    SolrInputDocument document = new SolrInputDocument();
    for (Map.Entry<String, Object> entry : mapWChild.entrySet()) {
      document.addField(entry.getKey(), entry.getValue());
    }
    assertEquals(
        IgnoreLargeDocumentProcessorFactory.ObjectSizeEstimator.estimate(document),
        estimate(mapWChild));

    SolrInputDocument childDocument = new SolrInputDocument();
    for (Map.Entry<String, Object> entry : mapWChild.entrySet()) {
      childDocument.addField(entry.getKey(), entry.getValue());
    }
    List<SolrInputDocument> childList =
        List.of(childDocument, new SolrInputDocument(childDocument));
    document.addField(childDocKey, childList);
    mapWChild.put(childDocKey, childList);
    assertEquals(
        IgnoreLargeDocumentProcessorFactory.ObjectSizeEstimator.estimate(document),
        estimate(mapWChild));
    assertEquals(
        IgnoreLargeDocumentProcessorFactory.ObjectSizeEstimator.estimate(document),
        estimate(childMap) * (childList.size() + 1) + estimate(childDocKey));
  }
}
