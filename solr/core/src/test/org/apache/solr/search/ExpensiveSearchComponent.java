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
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;

public class ExpensiveSearchComponent extends SearchComponent {

  public static final String MEM_LOAD_COUNT_PARAM = "memLoadCount";
  public static final String CPU_LOAD_COUNT_PARAM = "cpuLoadCount";
  public static final String SLEEP_MS_PARAM = "sleepMs";

  private static final KeyPairGenerator kpg;

  static {
    KeyPairGenerator generator;
    try {
      generator = KeyPairGenerator.getInstance("RSA");
    } catch (NoSuchAlgorithmException e) {
      generator = null;
    }
    kpg = generator;
  }

  final ArrayList<String> data = new ArrayList<>();

  @Override
  public void prepare(ResponseBuilder rb) throws IOException {}

  @Override
  public void process(ResponseBuilder rb) throws IOException {
    final long cpuLoadCount = rb.req.getParams().getLong(CPU_LOAD_COUNT_PARAM, 0L);
    final long sleepMs = rb.req.getParams().getLong(SLEEP_MS_PARAM, 0);
    final int memLoadCount = rb.req.getParams().getInt(MEM_LOAD_COUNT_PARAM, 0);
    data.clear();
    KeyPair kp = null;
    // create memory load
    for (int j = 0; j < memLoadCount; j++) {
      String str = TestUtil.randomUnicodeString(LuceneTestCase.random(), 100);
      data.add(str);
    }
    // create CPU load
    for (int i = 0; i < cpuLoadCount; i++) {
      if (kpg == null) {
        throw new RuntimeException("cannot generate consistent CPU load on this JVM.");
      }
      kpg.initialize(1024);
      kp = kpg.generateKeyPair();
    }
    if (kp != null) {
      rb.rsp.add("keyPair", kp.getPublic().toString());
    }
    // create wall-clock load
    if (sleepMs > 0) {
      try {
        Thread.sleep(sleepMs);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public String getDescription() {
    return "expensive";
  }
}
