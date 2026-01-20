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
package org.apache.solr.servlet;

import jakarta.servlet.FilterConfig;
import jakarta.servlet.ServletException;
import jakarta.servlet.UnavailableException;
import jakarta.servlet.http.HttpFilter;
import java.lang.invoke.MethodHandles;
import org.apache.solr.core.CoreContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A superclass for filters that will need to interact with the CoreContainer. */
public abstract class CoreContainerAwareHttpFilter extends HttpFilter {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private volatile CoreContainerProvider containerProvider;

  @Override
  public void init(FilterConfig config) throws ServletException {
    containerProvider = CoreContainerProvider.serviceForContext(config.getServletContext());
    if (log.isTraceEnabled()) {
      log.trace("{}.init(): {}", this.getClass().getName(), this.getClass().getClassLoader());
    }
  }

  /**
   * The CoreContainer. It's guaranteed to be constructed before this filter is initialized, but
   * could have been shut down. Never null.
   */
  public CoreContainer getCores() throws UnavailableException {
    return containerProvider.getCoreContainer();
  }
}
