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

package org.apache.solr.client.solrj.impl;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.common.util.NamedList;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;

public interface PauseableHttpSolrClient {

    final Semaphore wait = new Semaphore(1);

    default public void pause() {
        try {
            wait.acquire();
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    default public void unPause() {
        wait.release();
    }

    public CompletableFuture<NamedList<Object>> requestAsync(final SolrRequest<?> solrRequest, String collection) ;

}
