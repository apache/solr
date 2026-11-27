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
package org.apache.solr.util;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import javax.xml.parsers.DocumentBuilderFactory;
import org.apache.solr.SolrTestCase;
import org.apache.solr.common.ConfigNode;
import org.junit.Test;
import org.xml.sax.InputSource;

/** Child iteration order in {@link DataConfigNode}. */
public class TestDataConfigNode extends SolrTestCase {

  private static ConfigNode parse(String xml) throws Exception {
    return new DataConfigNode(
        new DOMConfigNode(
            DocumentBuilderFactory.newInstance()
                .newDocumentBuilder()
                .parse(new InputSource(new StringReader(xml)))
                .getDocumentElement()));
  }

  @Test
  public void testChildrenInDocumentOrder() throws Exception {
    ConfigNode root = parse("<r><a n=\"1\"/><b n=\"2\"/><a n=\"3\"/></r>");
    List<String> seen = new ArrayList<>();
    root.forEachChild(
        child -> {
          seen.add(child.name() + child.attr("n"));
          return Boolean.TRUE;
        });
    assertEquals(List.of("a1", "b2", "a3"), seen);
  }
}
