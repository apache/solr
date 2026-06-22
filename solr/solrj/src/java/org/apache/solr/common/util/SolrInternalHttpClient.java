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
import org.eclipse.jetty.io.ClientConnector;
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
      // Jetty 10.0.x AB-BA deadlock guard. The HTTP/2 client registers each HTTP2Session as a *managed*
      // bean (HTTP2ClientConnectionFactory.ConnectionListener.onOpened -> client.addManaged(session)).
      // When a connection closes, a selector thread runs the EOF cascade *while holding the SslConnection
      // lock inside fill()*: onClosed -> client.removeBean(session) -> stops the managed session -> needs
      // the session's AbstractLifeCycle lock. If this (stopping) thread is concurrently stopping that same
      // session it holds the session lifecycle lock and, via HTTP2Session.disconnect() -> endpoint close ->
      // graceful TLS close_notify flush, needs the SslConnection lock -> deadlock (confirmed present and
      // structurally unchanged through jetty 10.0.26). Quiesce the I/O selector threads FIRST by stopping
      // the ClientConnector: their close cascades then run while this thread holds no session lock, so by
      // the time super.doStop() stops the (already-closed) sessions there is no SSL flush left to contend
      // for the lock. AbstractLifeCycle.stop() is idempotent, so super's later stop of the now-STOPPED
      // connector is a no-op.
      int quiesced = 0;
      for (ClientConnector connector : getContainedBeans(ClientConnector.class)) {
        try {
          connector.stop();
          quiesced++;
        } catch (Exception e) {
          log.warn("Exception quiescing ClientConnector before HTTP client stop", e);
        }
      }
      if (quiesced == 0) {
        // No ClientConnector bean was found to quiesce first, so the AB-BA deadlock guard above is inert and
        // super.doStop() will stop the sessions without the selector threads being quiesced. This is not
        // expected for a started HTTP/2 client; warn so a Jetty bean-registration change does not silently
        // reintroduce the deadlock.
        log.warn("No ClientConnector bean found to quiesce before HTTP client stop; deadlock guard inactive");
      }
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
