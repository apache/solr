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

import java.io.IOException;
import java.util.ArrayList;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;

public class ExpensiveSearchComponent extends SearchComponent {

  @Override
  public void prepare(ResponseBuilder rb) throws IOException {}

  @Override
  public void process(ResponseBuilder rb) throws IOException {
    ArrayList<byte[]> data = new ArrayList<>();
    rb.req.getContext().put("__data__", data);
    final long spinWaitCount = rb.req.getParams().getLong("spinWaitCount", 1L);
    final long sleepMs = rb.req.getParams().getLong("sleepMs", 0);
    final int dataSize = rb.req.getParams().getInt("dataSize", 1);
    for (int i = 0; i < spinWaitCount; i++) {
      // create CPU load
      for (int j = 0; j < dataSize; j++) {
        // create mem load
        data.add(new byte[1000]);
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
