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
package org.apache.solr.handler.loader;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.request.JavaBinUpdateRequestCodec;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Utils;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.servlet.ResponseUtils;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.processor.BufferingRequestProcessor;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.util.LogLevel;
import org.apache.solr.util.LogListener;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JavabinLoaderTest extends SolrTestCaseJ4 {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @BeforeClass
  public static void beforeTests() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
  }

  /**
   * Verifies the isLastDocInBatch flag gets set correctly for a batch of docs and for a request
   * with a single doc.
   */
  public void testLastDocInBatchFlag() throws Exception {
    doTestLastDocInBatchFlag(1); // single doc
    doTestLastDocInBatchFlag(2); // multiple docs
  }

  protected void doTestLastDocInBatchFlag(int numDocsInBatch) throws Exception {
    List<SolrInputDocument> batch = new ArrayList<>(numDocsInBatch);
    for (int d = 0; d < numDocsInBatch; d++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.setField("id", String.valueOf(d));
      batch.add(doc);
    }

    UpdateRequest updateRequest = new UpdateRequest();
    if (batch.size() > 1) {
      updateRequest.add(batch);
    } else {
      updateRequest.add(batch.get(0));
    }

    // client-side SolrJ would do this ...
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    (new JavaBinUpdateRequestCodec()).marshal(updateRequest, os);

    // need to override the processAdd method b/c JavabinLoader calls
    // clear on the addCmd after it is passed on to the handler ... a simple clone will suffice for
    // this test
    BufferingRequestProcessor mockUpdateProcessor =
        new BufferingRequestProcessor(null) {
          @Override
          public void processAdd(AddUpdateCommand cmd) {
            addCommands.add((AddUpdateCommand) cmd.clone());
          }
        };

    SolrQueryRequest req = req();
    (new JavabinLoader())
        .load(
            req,
            new SolrQueryResponse(),
            new ContentStreamBase.ByteArrayStream(os.toByteArray(), "test"),
            mockUpdateProcessor);
    req.close();

    assertEquals(mockUpdateProcessor.addCommands.size(), numDocsInBatch);
    for (int i = 0; i < numDocsInBatch - 1; i++)
      assertFalse(mockUpdateProcessor.addCommands.get(i).isLastDocInBatch); // not last doc in batch

    // last doc should have the flag set
    assertTrue(mockUpdateProcessor.addCommands.get(batch.size() - 1).isLastDocInBatch);
  }

  @Test
  public void javabinLoader_denseVector_shouldIndexCorrectly() throws Exception {
    SolrInputDocument doc1 = new SolrInputDocument();
    doc1.addField("id", "555");
    doc1.addField("vector", Arrays.asList(1.4f, 2.4f, 3.4f, 4.4f));

    EmbeddedSolrServer solrJClient =
        new EmbeddedSolrServer(
            h.getCoreContainer(), "collection1", EmbeddedSolrServer.RequestWriterSupplier.JavaBin);
    solrJClient.add(doc1);
    solrJClient.commit();

    assertQ(
        req("q", "id:555", "fl", "vector"),
        "*[count(//doc)=1]",
        "//result/doc[1]/arr[@name=\"vector\"]/float[1][.='" + 1.4 + "']",
        "//result/doc[1]/arr[@name=\"vector\"]/float[2][.='" + 2.4 + "']",
        "//result/doc[1]/arr[@name=\"vector\"]/float[3][.='" + 3.4 + "']",
        "//result/doc[1]/arr[@name=\"vector\"]/float[4][.='" + 4.4 + "']");
  }

  @Test
  @LogLevel("org.apache.solr.handler.loader.JavabinLoader=INFO")
  public void testIOExceptionDoesNotLeakDocumentContents() throws Exception {
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", "7");
    doc.addField("name", "secret customer value");

    UpdateRequest updateRequest = new UpdateRequest();
    updateRequest.add(doc);

    ByteArrayOutputStream os = new ByteArrayOutputStream();
    new JavaBinUpdateRequestCodec().marshal(updateRequest, os);

    UpdateRequestProcessor failingProcessor =
        new BufferingRequestProcessor(null) {
          @Override
          public void processAdd(AddUpdateCommand cmd) throws IOException {
            throw new IOException("simulated failure for secret customer value");
          }
        };

    SolrQueryRequest req = req();
    try {
      SolrException ex =
          expectThrows(
              SolrException.class,
              () ->
                  new JavabinLoader()
                      .load(
                          req,
                          new SolrQueryResponse(),
                          new ContentStreamBase.ByteArrayStream(os.toByteArray(), "test"),
                          failingProcessor));
      assertEquals("ERROR adding document [doc=7]", ex.getMessage());
      assertFalse(ex.getMessage().contains("secret customer value"));
      assertEquals(500, ex.code());

      try (LogListener errors = LogListener.error(JavabinLoader.class)) {
        var logger = LoggerFactory.getLogger(JavabinLoader.class);
        NamedList<Object> errorInfo = new NamedList<>();
        assertEquals(500, ResponseUtils.getErrorInfo(ex, errorInfo, logger, false));
        assertNotNull(
            "Exercise response traces, not just the outer message", errorInfo.get("trace"));
        assertFalse(Utils.toJSONString(errorInfo).contains("secret customer value"));
        var typedErrorInfo = ResponseUtils.getTypedErrorInfo(ex, logger, false);
        assertNotNull(typedErrorInfo.trace);
        assertFalse(Utils.toJSONString(typedErrorInfo).contains("secret customer value"));
        assertEquals(2, errors.getCount());
        while (!errors.getQueue().isEmpty()) {
          var event = errors.getQueue().poll();
          assertNotNull(event.getThrown());
          StringWriter trace = new StringWriter();
          event.getThrown().printStackTrace(new PrintWriter(trace));
          assertTrue(trace.toString().contains("ERROR adding document [doc=7]"));
          assertFalse(trace.toString().contains("secret customer value"));
        }
      }
      assertNull(ex.getCause());
    } finally {
      req.close();
    }
  }
}
