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
package org.apache.solr.logging;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.response.BinaryQueryResponseWriter;
import org.apache.solr.response.JSONResponseWriter;
import org.apache.solr.response.JacksonJsonWriter;
import org.apache.solr.response.QueryResponseWriter;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.TimeOut;
import org.junit.Before;
import org.junit.Test;
import org.noggit.CharArr;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestLogWatcher extends SolrTestCaseJ4 {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private LogWatcherConfig config;

  @Before
  public void before() {
    config = new LogWatcherConfig(true, null, "INFO", 1);
  }

  // Create several log watchers and ensure that new messages go to the new watcher.
  // NOTE: Since the superclass logs messages, it's possible that there are one or more
  //       messages in the queue at the start, especially with async logging.
  //       All we really care about is that new watchers get the new messages, so test for that
  //       explicitly. See SOLR-12732.
  @Test
  public void testLog4jWatcher() throws InterruptedException, IOException {
    LogWatcher<?> watcher = null;
    int lim = random().nextInt(3) + 2;
    // Every time through this loop, ensure that, of all the test messages that have been logged,
    // only the current test message is present. NOTE: there may be log messages from the superclass
    // the first time around.
    List<String> oldMessages = new ArrayList<>(lim);
    for (int idx = 0; idx < lim; ++idx) {

      watcher = LogWatcher.newRegisteredLogWatcher(config, null);

      // Now log a message and ensure that the new watcher sees it.
      String msg = "This is a test message: " + idx;
      log.warn(msg);

      // Loop to give the logger time to process the async message and notify the new watcher.
      TimeOut timeOut = new TimeOut(10, TimeUnit.SECONDS, TimeSource.NANO_TIME);
      boolean foundNewMsg = false;
      boolean foundOldMessage = false;
      // In local testing this loop usually succeeds 1-2 tries, so it's not very expensive to loop.
      QueryResponseWriter responseWriter =
          random().nextBoolean() ? new JacksonJsonWriter() : new JSONResponseWriter();
      do {
        // Returns an empty (but non-null) list even if there are no messages yet.
        SolrDocumentList events = watcher.getHistory(-1, null);
        validateWrite(responseWriter, events, msg);
        for (SolrDocument doc : events) {
          String oneMsg = (String) doc.get("message");
          if (oneMsg.equals(msg)) {
            foundNewMsg = true;
          }
          // Check that no old messages bled over into this watcher.
          for (String oldMsg : oldMessages) {
            if (oneMsg.equals(oldMsg)) {
              foundOldMessage = true;
            }
          }
        }
        if (foundNewMsg == false) {
          Thread.sleep(10);
        }
      } while (foundNewMsg == false && timeOut.hasTimedOut() == false);

      if (foundNewMsg == false || foundOldMessage) {
        System.out.println("Dumping all events in failed watcher:");
        SolrDocumentList events = watcher.getHistory(-1, null);
        for (SolrDocument doc : events) {
          System.out.println("   Event:'" + doc.toString() + "'");
        }
        System.out.println("Recorded old messages");
        for (String oldMsg : oldMessages) {
          System.out.println("    " + oldMsg);
        }

        fail(
            "Did not find expected message state, dumped current watcher's messages above, last message added: '"
                + msg
                + "'");
      }
      oldMessages.add(msg);
    }
  }

  /**
   * Here we validate that serialization works as expected for several different methods. Ideally we
   * would use actual serialization from Jersey/Jackson, since this is what really happens in V2
   * APIs. But this is simpler, and should give us roughly equivalent assurances.
   */
  private static void validateWrite(
      QueryResponseWriter responseWriter, SolrDocumentList docs, String expectMsg)
      throws IOException {
    SolrQueryRequest req = new SolrQueryRequestBase(null, new ModifiableSolrParams()) {};
    SolrQueryResponse rsp = new SolrQueryResponse();
    rsp.addResponse(docs);
    String output;
    if (responseWriter instanceof BinaryQueryResponseWriter) {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ((BinaryQueryResponseWriter) responseWriter).write(baos, req, rsp);
      baos.close();
      output = baos.toString(StandardCharsets.UTF_8);
    } else {
      StringWriter writer = new StringWriter();
      responseWriter.write(writer, req, rsp);
      writer.close();
      output = writer.toString();
    }
    assertTrue("found: " + output, output.contains(expectMsg));
    validateWrite(docs, expectMsg);
  }

  private static void validateWrite(SolrDocumentList docs, String expectMsg) throws IOException {
    CharArr arr = new CharArr();
    org.noggit.JSONWriter w = new org.noggit.JSONWriter(arr, 2);
    docs.writeMap(
        new MapWriter.EntryWriter() {
          boolean first = true;

          @Override
          public MapWriter.EntryWriter put(CharSequence k, Object v) {
            if (first) {
              first = false;
            } else {
              w.writeValueSeparator();
            }
            w.indent();
            w.writeString(k.toString());
            w.writeNameSeparator();
            w.write(v);
            return this;
          }
        });
    String output = arr.toString();
    assertTrue("found: " + output, output.contains(expectMsg));
  }
}
