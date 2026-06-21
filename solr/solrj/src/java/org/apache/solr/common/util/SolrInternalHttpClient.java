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
package org.apache.solr.common.util;

import org.agrona.MutableDirectBuffer;
import org.apache.solr.client.solrj.impl.GenericUrl;
import org.apache.solr.client.solrj.impl.SolrHttpRequest;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.HttpClientTransport;
import org.eclipse.jetty.client.HttpConversation;
import org.eclipse.jetty.client.HttpDestination;
import org.eclipse.jetty.client.HttpRequest;
import org.eclipse.jetty.client.Origin;
import org.eclipse.jetty.client.api.Destination;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.api.Response;
import org.eclipse.jetty.http.HttpField;
import org.jctools.maps.NonBlockingHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SolrInternalHttpClient extends HttpClient {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final Map<Origin,HttpDestination> dests = new NonBlockingHashMap<>(128);

  public SolrInternalHttpClient(HttpClientTransport transport) {
    super(transport);
    assert ObjectReleaseTracker.getInstance().track(this);
  }

  public HttpDestination resolveDestination(Origin origin) {
    return dests.computeIfAbsent(origin, o -> {
      HttpDestination newDestination = getTransport().newHttpDestination(o);
      addManaged(newDestination);
      if (log.isDebugEnabled()) log.debug("Created {}", newDestination);
      return newDestination;
    });
  }

  public HttpDestination getDestination(Origin origin) {
    return dests.get(origin);
  }

  public Map<Origin,HttpDestination> getDestinationsMap() {
    return dests;
  }
//
//
  public boolean removeDestination(HttpDestination destination) {
    super.removeDestination(destination);
    removeBean(destination);
    return dests.remove(destination.getOrigin(), destination);
  }

  public HttpField getAcceptEncodingField()
  {
    return super.getAcceptEncodingField();
  }

  public static void send(final Request request, List<Response.ResponseListener> listeners)
  {
    send(request, listeners);
  }


  public List<Destination> getDestinations() {
    return new ArrayList<>(dests.values());
  }

  public Request copyRequest(HttpRequest oldRequest, URI newURI)
  {
    return super.copyRequest(oldRequest, newURI);
  }

  public SolrHttpRequest newSolrRequest(String uri, MutableDirectBuffer buffer)
  {

    return new SolrHttpRequest(this,  new HttpConversation(), uri, buffer);
  }

  @Override protected void doStop() throws Exception {
    if (log.isDebugEnabled()) {
      log.debug("Stopping {}", this.getClass().getSimpleName());
    }
    try {
      super.doStop();
      for (HttpDestination destination : dests.values()) {
        destination.close();
      }
      dests.clear();
    } finally {
      assert ObjectReleaseTracker.getInstance().release(this);
    }
  }

}
