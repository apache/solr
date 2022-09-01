/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.jersey;

import java.lang.invoke.MethodHandles;
import org.glassfish.jersey.server.monitoring.ApplicationEvent;
import org.glassfish.jersey.server.monitoring.ApplicationEventListener;
import org.glassfish.jersey.server.monitoring.RequestEvent;
import org.glassfish.jersey.server.monitoring.RequestEventListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Logs out application-level information useful for troubleshooting Jersey development.
 *
 * @see RequestEventLogger
 */
public class ApplicationEventLogger implements ApplicationEventListener {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private volatile long requestCount = 0;

  @Override
  public void onEvent(ApplicationEvent event) {
    if (log.isInfoEnabled()) {
      log.info("Received ApplicationEvent {}", event.getType());
    }
  }

  @Override
  public RequestEventListener onRequest(RequestEvent requestEvent) {
    requestCount++;
    log.info("Starting Jersey request {}", requestCount);
    return new RequestEventLogger(requestCount);
  }
}
