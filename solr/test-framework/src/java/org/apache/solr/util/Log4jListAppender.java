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

package org.apache.solr.util;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.solr.common.util.SuppressForbidden;

import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.impl.MutableLogEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maintains an in memory List of log events.
 * <p>
 * Inspired by <code>org.apache.logging.log4j.core.test.appender.ListAppender</code>
 * but we have much simpler needs.
 * </p>
 *
 * TODO: Refactor into something easier to use (SOLR-15629)
 * @lucene.internal
 * @lucene.experimental  
 */
@SuppressForbidden(reason="We need to use log4J2 classes directly to check that the ErrorLogMuter is working")
public final class Log4jListAppender extends AbstractAppender {
  
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  // Use Collections.synchronizedList rather than CopyOnWriteArrayList because we expect
  // more frequent writes than reads.
  private final List<LogEvent> events = Collections.synchronizedList(new ArrayList<>());
  private final List<LogEvent> publicEvents = Collections.unmodifiableList(events);
  
  public Log4jListAppender(final String name) {
    super(name, null, null, true, Property.EMPTY_ARRAY);
    assert null != name;
    this.start();
  }
  
  @Override
  public void append(final LogEvent event) {
    if (event instanceof MutableLogEvent) {
      // must take snapshot or subsequent calls to logger.log() will modify this event
      events.add(((MutableLogEvent) event).createMemento());
    } else {
      events.add(event);
    }
    if (log.isDebugEnabled()) {
      log.debug("{} intercepted a log event (#{})", this.getName(), events.size());
    }
  }
  
  /** Returns an immutable view of captured log events, contents can change as events are logged */
  public List<LogEvent> getEvents() {
    return publicEvents;
  }
}
