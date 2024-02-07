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
package org.apache.solr.search;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;

import java.io.IOException;
import java.util.ArrayList;

public class ExpensiveSearchComponent extends SearchComponent {
  final ArrayList<String> data = new ArrayList<>();

  @Override
  public void prepare(ResponseBuilder rb) throws IOException {

  }

  @Override
  public void process(ResponseBuilder rb) throws IOException {
    final long spinWaitCount = rb.req.getParams().getLong("spinWaitCount", 0L);
    final long sleepMs = rb.req.getParams().getLong("sleepMs", 0);
    final int dataSize = rb.req.getParams().getInt("dataSize", 1);
    for (int i = 0; i < spinWaitCount; i++) {
      data.clear();
      // create CPU load
      for (int j = 0; j < dataSize; j++) {
        String str = TestUtil.randomUnicodeString(LuceneTestCase.random(), 100);
        // create mem load
        data.add(str);
      }
    }
    // create wall-clock load
    try {
      Thread.sleep(sleepMs);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String getDescription() {
    return "expensive";
  }
}
