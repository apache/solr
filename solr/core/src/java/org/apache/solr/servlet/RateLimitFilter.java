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

import com.google.common.annotations.VisibleForTesting;
import jakarta.servlet.FilterChain;
import jakarta.servlet.FilterConfig;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import org.apache.solr.common.SolrException;

public class RateLimitFilter extends CoreContainerAwareHttpFilter {
  private RateLimitManager rateLimitManager;

  @Override
  public void init(FilterConfig config) throws ServletException {
    super.init(config);
    RateLimitManager.Builder builder = new RateLimitManager.Builder();
    builder.withZk(getCores().getZkController());
    this.rateLimitManager = builder.build();
  }

  @Override
  protected void doFilter(HttpServletRequest req, HttpServletResponse res, FilterChain chain)
      throws IOException, ServletException {
    RateLimitManager rateLimitManager = getRateLimitManager();
    try (RequestRateLimiter.SlotReservation accepted = rateLimitManager.handleRequest(req)) {
      if (accepted == null) {
        res.sendError(
            SolrException.ErrorCode.TOO_MANY_REQUESTS.code, RateLimitManager.ERROR_MESSAGE);
        return;
      }
      chain.doFilter(req, res);
    } catch (InterruptedException e1) {
      Thread.currentThread().interrupt();
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e1.getMessage());
    }
  }

  public RateLimitManager getRateLimitManager() {
    return rateLimitManager;
  }

  @VisibleForTesting
  void setRateLimitManager(RateLimitManager rateLimitManager) {
    this.rateLimitManager = rateLimitManager;
  }
}
