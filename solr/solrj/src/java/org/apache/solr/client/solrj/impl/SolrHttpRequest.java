package org.apache.solr.client.solrj.impl;

import org.agrona.MutableDirectBuffer;
import org.apache.solr.common.util.ExpandableBuffers;
import org.apache.solr.common.util.SolrInternalHttpClient;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.HttpConversation;
import org.eclipse.jetty.client.HttpRequest;
import org.eclipse.jetty.client.api.Request;

import java.net.URI;
import java.util.concurrent.atomic.AtomicBoolean;

public class SolrHttpRequest extends HttpRequest {
  private final MutableDirectBuffer buffer;
  // Guards against releasing the request body buffer to the shared pool more than once.
  // The buffer is returned to ExpandableBuffers when the exchange finishes, and several
  // independent completion callbacks (request onComplete, response listener finally) may all
  // try to free it. Releasing the same buffer twice puts it in the pool's queue twice, so two
  // concurrent requests then acquire the *same* direct buffer and marshal into it at once,
  // corrupting the outgoing javabin stream (the receiver fails with OOM in readStr or
  // "Invalid version (expected 3, but N)"). Free exactly once.
  private final AtomicBoolean freed = new AtomicBoolean(false);

  public SolrHttpRequest(SolrInternalHttpClient client, HttpConversation conversation, String uri, MutableDirectBuffer buffer) {
    super(client, conversation, URI.create(uri));
    this.buffer = buffer;
  }

  public void freeBuffer() {
    if (buffer != null && freed.compareAndSet(false, true)) {
      ExpandableBuffers.getInstance().release(buffer);
    }
  }
}
