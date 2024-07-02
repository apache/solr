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
package org.apache.solr.crossdc.common;

import java.util.Arrays;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.junit.Test;

public class MirroredSolrRequestSerializerTest extends SolrTestCase {

  private static final byte[] EMPTY_ARR = new byte[3];

  @Test
  public void testSerializationBufferOptimization() throws Exception {
    MirroredSolrRequestSerializer serializer = new MirroredSolrRequestSerializer();
    UpdateRequest req = new UpdateRequest();
    SolrInputDocument doc = new SolrInputDocument();
    req.add(doc);
    for (int i = 0; i < 100; i++) {
      // very small docs produce trailing zeroes due to the optimization in
      // ExposedByteArrayOutputStream
      String fieldValue = TestUtil.randomRealisticUnicodeString(random(), i * 100, i * 100);
      doc.setField("test", fieldValue);
      MirroredSolrRequest<?> mirroredRequest = new MirroredSolrRequest<>(req);
      byte[] data = serializer.serialize("test", mirroredRequest);
      if (Arrays.equals(
          Arrays.copyOfRange(data, data.length - EMPTY_ARR.length, data.length), EMPTY_ARR)) {
        System.err.println("TRAILING ZEROES! buf len=" + data.length);
      }
      // fortunately deserialization skips these trailing zeroes
      MirroredSolrRequest<?> deserialized = serializer.deserialize("test", data);
      String deserValue =
          (String)
              ((UpdateRequest) deserialized.getSolrRequest())
                  .getDocuments()
                  .get(0)
                  .getFieldValue("test");
      assertEquals(fieldValue, deserValue);
    }
  }
}
