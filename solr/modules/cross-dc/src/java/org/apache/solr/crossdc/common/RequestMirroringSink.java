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
package org.apache.solr.crossdc.common;

public interface RequestMirroringSink {

  /**
   * Submits a mirrored solr request to the appropriate end-point such that it is eventually
   * received by solr A direct sink may simply use CloudSolrServer to process requests directly. A
   * queueing sink will serialize the request and submit it to a queue for later consumption
   *
   * @param request the request that is to be mirrored
   * @throws MirroringException Implementations may throw an exception
   */
  void submit(final MirroredSolrRequest<?> request) throws MirroringException;

  void submitToDlq(final MirroredSolrRequest<?> request) throws MirroringException;
}
