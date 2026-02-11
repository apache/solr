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
package org.apache.solr.crossdc.manager.consumer;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Locale;

public class HealthCheckServlet extends HttpServlet {
  private static final long serialVersionUID = -7848291432584409313L;

  public static final String KAFKA_CROSSDC_CONSUMER =
      HealthCheckServlet.class.getName() + ".kafkaCrossDcConsumer";

  private KafkaCrossDcConsumer consumer;

  @Override
  public void init() throws ServletException {
    consumer = (KafkaCrossDcConsumer) getServletContext().getAttribute(KAFKA_CROSSDC_CONSUMER);
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    if (consumer == null) {
      resp.sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
      return;
    }
    boolean kafkaConnected = consumer.isKafkaConnected();
    boolean solrConnected = consumer.isSolrConnected();
    boolean running = consumer.isRunning();
    String content =
        String.format(
            Locale.ROOT,
            "{\n  \"kafka\": %s,\n  \"solr\": %s,\n  \"running\": %s\n}",
            kafkaConnected,
            solrConnected,
            running);
    resp.setContentType("application/json");
    resp.setHeader("Cache-Control", "must-revalidate,no-cache,no-store");
    resp.setCharacterEncoding("UTF-8");
    resp.getOutputStream().write(content.getBytes(StandardCharsets.UTF_8));
    if (kafkaConnected && solrConnected && running) {
      resp.setStatus(HttpServletResponse.SC_OK);
    } else {
      resp.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
    }
  }
}
