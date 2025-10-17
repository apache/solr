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
package org.apache.solr.crossdc.update.processor;

import java.lang.invoke.MethodHandles;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.crossdc.common.KafkaMirroringSink;
import org.apache.solr.crossdc.common.MirroredSolrRequest;
import org.apache.solr.crossdc.common.MirroringException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaRequestMirroringHandler implements RequestMirroringHandler {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  final KafkaMirroringSink sink;

  public KafkaRequestMirroringHandler(KafkaMirroringSink sink) {
    log.debug("create KafkaRequestMirroringHandler");
    this.sink = sink;
  }

  /**
   * When called, should handle submitting the request to the queue
   *
   * @param request to mirror
   */
  @Override
  public void mirror(UpdateRequest request) throws MirroringException {
    if (log.isTraceEnabled()) {
      log.trace(
          "submit update to sink docs={}, deletes={}, params={}",
          request.getDocuments(),
          request.getDeleteById(),
          request.getParams());
    }
    // TODO: Enforce external version constraint for consistent update replication (cross-cluster)
    final MirroredSolrRequest<?> mirroredRequest =
        new MirroredSolrRequest<>(MirroredSolrRequest.Type.UPDATE, 1, request, System.nanoTime());
    try {
      sink.submit(mirroredRequest);
    } catch (MirroringException exception) {
      if (log.isInfoEnabled()) {
        log.info("Sending message to dead letter queue");
      }
      sink.submitToDlq(mirroredRequest);
      throw new MirroringException(exception);
    }
  }
}
