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
import java.lang.invoke.MethodHandles;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.interfaces.RSAPublicKey;
import java.util.ArrayList;
import java.util.Arrays;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.handler.component.ShardRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A search component used for testing "expensive" operations, i.e. those that take long wall-clock
 * time, or consume a lot of CPU or memory. Depending on the {@link #STAGES_PARAM} this load can be
 * generated at various stages in the distributed query processing.
 *
 * <p>This component can be used in <code>solrconfig.xml</code> like this:
 *
 * <pre>{@code
 * <config>
 *   ...
 *   <searchComponent name="expensiveSearchComponent"
 *                    class="org.apache.solr.search.ExpensiveSearchComponent"/>
 *   ...
 *   <requestHandler name="/select" class="solr.SearchHandler">
 *     <arr name="first-components">
 *       <str>expensiveSearchComponent</str>
 *     </arr>
 *     ...
 *   </requestHandler>
 * </config>
 * }</pre>
 *
 * For example, if the request parameters are as follows:
 *
 * <pre>{@code
 * sleepMs=100&memLoadCount=100&cpuLoadCount=10&stages=prepare,process
 * }</pre>
 *
 * the component will introduce a 100ms delay, allocate ~100 KiB and consume around 500ms of CPU
 * time both in the "prepare" and in the "process" stages of the distributed query processing.
 */
public class ExpensiveSearchComponent extends SearchComponent {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * Generate memory load by allocating this number of random unicode strings, 100 characters each.
   */
  public static final String MEM_LOAD_COUNT_PARAM = "memLoadCount";

  /** Generate CPU load by repeatedly running an expensive computation (RSA key-pair generation). */
  public static final String CPU_LOAD_COUNT_PARAM = "cpuLoadCount";

  /** Generate a wall-clock delay by sleeping this number of milliseconds. */
  public static final String SLEEP_MS_PARAM = "sleepMs";

  /** Comma-separated list of stages where the load will be generated. */
  public static final String STAGES_PARAM = "stages";

  public static final String STAGE_PREPARE = "prepare";
  public static final String STAGE_PROCESS = "process";
  public static final String STAGE_FINISH = "finish";
  public static final String STAGE_DISTRIB_PROCESS = "distrib";
  public static final String STAGE_HANDLE_RESPONSES = "handle";

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

  final ArrayList<byte[]> data = new ArrayList<>();

  private void generateLoad(ResponseBuilder rb, String stage) {
    if (log.isTraceEnabled()) {
      log.trace(
          "-- {} generateLoad(): params: {} --\n{}",
          stage,
          rb.req.getParams().toString(),
          new Exception());
    }
    final long cpuLoadCount = rb.req.getParams().getLong(CPU_LOAD_COUNT_PARAM, 0L);
    final long sleepMs = rb.req.getParams().getLong(SLEEP_MS_PARAM, 0);
    final int memLoadCount = rb.req.getParams().getInt(MEM_LOAD_COUNT_PARAM, 0);
    data.clear();
    KeyPair kp = null;
    // create memory load
    if (memLoadCount > 0) {
      if (log.isTraceEnabled()) {
        log.trace("--- STAGE {}: creating mem load {}", stage, memLoadCount);
      }
      for (int j = 0; j < memLoadCount; j++) {
        byte[] chunk = new byte[1024];
        Arrays.fill(chunk, (byte) 1);
        data.add(chunk);
      }
    }
    // create CPU load
    if (cpuLoadCount > 0) {
      if (log.isTraceEnabled()) {
        log.trace("--- STAGE {}: creating CPU load {}", stage, cpuLoadCount);
      }
      for (int i = 0; i < cpuLoadCount; i++) {
        if (kpg == null) {
          throw new RuntimeException("cannot generate consistent CPU load on this JVM.");
        }
        kpg.initialize(1024);
        kp = kpg.generateKeyPair();
      }
    }
    if (kp != null) {
      RSAPublicKey key = (RSAPublicKey) kp.getPublic();
      rb.rsp.add(
          "keyPair-" + stage,
          key.getAlgorithm() + " " + key.getFormat() + " " + key.getModulus().bitLength());
    }
    // create wall-clock load
    if (sleepMs > 0) {
      if (log.isTraceEnabled()) {
        log.trace("--- STAGE {}: creating wall-clock load {}", stage, sleepMs);
      }
      try {
        Thread.sleep(sleepMs);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private static boolean hasStage(ResponseBuilder rb, String stageName) {
    String stages = rb.req.getParams().get(STAGES_PARAM);
    if (stages == null) {
      return false;
    } else {
      // no need to split on commas, stage names are unique
      return stages.contains(stageName);
    }
  }

  @Override
  public void prepare(ResponseBuilder rb) throws IOException {
    if (hasStage(rb, STAGE_PREPARE)) {
      generateLoad(rb, STAGE_PREPARE);
    }
  }

  @Override
  public void process(ResponseBuilder rb) throws IOException {
    if (hasStage(rb, STAGE_PROCESS)) {
      generateLoad(rb, STAGE_PROCESS);
    }
  }

  @Override
  public void finishStage(ResponseBuilder rb) {
    if (hasStage(rb, STAGE_FINISH)) {
      generateLoad(rb, STAGE_FINISH);
    }
  }

  @Override
  public int distributedProcess(ResponseBuilder rb) throws IOException {
    if (hasStage(rb, STAGE_DISTRIB_PROCESS)) {
      generateLoad(rb, STAGE_DISTRIB_PROCESS);
    }
    return ResponseBuilder.STAGE_DONE;
  }

  @Override
  public void handleResponses(ResponseBuilder rb, ShardRequest sreq) {
    if (hasStage(rb, STAGE_HANDLE_RESPONSES)) {
      generateLoad(rb, STAGE_HANDLE_RESPONSES + " " + sreq);
    }
  }

  @Override
  public String getDescription() {
    return "expensive";
  }
}
