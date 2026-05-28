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
package org.apache.solr.security;

import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wires the Solr security agent's violation reporter to SLF4J once Log4j2 is available. Uses
 * reflection because {@code solr:agent-sm} and {@code solr:core} have no compile-time dependency on
 * each other. Call {@link #wire()} from {@code CoreContainer} after Log4j2 is initialised; from
 * that point violations appear in {@code solr.log} rather than {@code System.err}. Safe to call
 * when the agent JAR is absent — {@link ClassNotFoundException} is silently ignored.
 */
public final class AgentViolationBridge {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private AgentViolationBridge() {}

  /**
   * Wires the security agent violation reporter to SLF4J. No-op if the agent JAR is not present.
   */
  public static void wire() {
    try {
      // SecurityViolationLogger is in the bootstrap classloader via Boot-Class-Path.
      Class<?> cls = Class.forName("org.apache.solr.security.agent.SecurityViolationLogger");
      Field f = cls.getField("reporter");
      Consumer<String> bridge = msg -> log.warn("SECURITY VIOLATION {}", msg);
      f.set(null, bridge);
      log.info("Security agent violation reporter wired to SLF4J");
    } catch (ClassNotFoundException e) {
      // Agent JAR not loaded — security agent is inactive, nothing to wire
    } catch (Exception e) {
      log.warn("Could not wire security agent violation reporter to SLF4J", e);
    }
  }
}
