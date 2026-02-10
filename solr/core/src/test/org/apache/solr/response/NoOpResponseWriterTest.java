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
package org.apache.solr.response;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import org.junit.Test;

public class NoOpResponseWriterTest {

  @Test
  public void testWrite() throws IOException {
    NoOpResponseWriter writer = new NoOpResponseWriter();

    Writer stringWriter = new StringWriter();

    writer.write(stringWriter, null, null);

    assertEquals(NoOpResponseWriter.MESSAGE, stringWriter.toString());
  }

  @Test
  public void testGetContentType() {
    NoOpResponseWriter writer = new NoOpResponseWriter();

    String contentType = writer.getContentType(null, null);
    assertEquals(QueryResponseWriter.CONTENT_TYPE_TEXT_UTF8, contentType);
  }
}
