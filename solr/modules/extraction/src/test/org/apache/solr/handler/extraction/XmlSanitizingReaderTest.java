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
package org.apache.solr.handler.extraction;

import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import org.apache.solr.SolrTestCaseJ4;
import org.junit.Test;

public class XmlSanitizingReaderTest extends SolrTestCaseJ4 {

  private static String sanitize(String s) throws Exception {
    Reader r = new XmlSanitizingReader(new StringReader(s));
    StringWriter w = new StringWriter();
    char[] buf = new char[16];
    int n;
    while ((n = r.read(buf)) != -1) {
      w.write(buf, 0, n);
    }
    r.close();
    return w.toString();
  }

  @Test
  public void testDropsNullNumericEntities() throws Exception {
    assertEquals("ab", sanitize("a&#0;b"));
    assertEquals("ac", sanitize("a&#00;c"));
    assertEquals("ad", sanitize("a&#x0;d"));
    assertEquals("ae", sanitize("a&#x0000;e"));
  }

  @Test
  public void testPassThroughNonNullEntitiesAndText() throws Exception {
    assertEquals("&amp; &#x41; &#65;", sanitize("&amp; &#x41; &#65;"));
    assertEquals("pre&#12post", sanitize("pre&#12post")); // unterminated non-zero
    assertEquals("abc", sanitize("abc"));
  }
}
