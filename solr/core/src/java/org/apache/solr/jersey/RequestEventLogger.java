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
import org.glassfish.jersey.server.monitoring.RequestEvent;
import org.glassfish.jersey.server.monitoring.RequestEventListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Logs out request-specific information useful for troubleshooting Jersey development.
 *
 * @see ApplicationEventLogger
 */
public class RequestEventLogger implements RequestEventListener {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private volatile long requestCount;

  public RequestEventLogger(long id) {
    this.requestCount = id;
  }

  @Override
  public void onEvent(RequestEvent event) {
    if (log.isInfoEnabled()) {
      log.info("Received event for request #{}: {}", requestCount, event.getType());
    }
    if (event.getType().equals(RequestEvent.Type.ON_EXCEPTION)) {
      log.error("Exception encountered executing Jersey request: ", event.getException());
    }
  }
}
