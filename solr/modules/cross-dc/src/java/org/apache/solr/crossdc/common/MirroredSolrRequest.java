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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Locale;
import java.util.Objects;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.ConfigSetAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.ConfigSetAdminResponse;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ContentStreamBase;

/**
 * Class to encapsulate a mirrored Solr request. This adds a timestamp and #attempts to the request
 * for tracking purpose.
 */
public class MirroredSolrRequest<T extends SolrResponse> {

  public enum Type {
    UPDATE,
    ADMIN,
    CONFIGSET,
    UNKNOWN;

    public static final Type get(String s) {
      if (s == null) {
        return UNKNOWN;
      } else {
        return Type.valueOf(s.toUpperCase(Locale.ROOT));
      }
    }
  }

  public static class MirroredAdminRequest extends CollectionAdminRequest<CollectionAdminResponse> {
    private ModifiableSolrParams params;

    public MirroredAdminRequest(
        CollectionParams.CollectionAction action, ModifiableSolrParams params) {
      super(action);
      this.params = params;
    }

    @Override
    public SolrParams getParams() {
      return params;
    }

    public void setParams(ModifiableSolrParams params) {
      this.params = params;
    }

    @Override
    protected CollectionAdminResponse createResponse(SolrClient client) {
      return new CollectionAdminResponse();
    }
  }

  public static class MirroredConfigSetRequest
      extends ConfigSetAdminRequest<MirroredConfigSetRequest, ConfigSetAdminResponse> {
    private Collection<ContentStream> contentStreams;
    private ModifiableSolrParams params;

    public MirroredConfigSetRequest(
        METHOD method, SolrParams params, Collection<ContentStream> contentStreams) {
      this.setMethod(method);
      this.params = ModifiableSolrParams.of(params);
      this.contentStreams = contentStreams;
      if (method == METHOD.POST && (contentStreams == null || contentStreams.isEmpty())) {
        throw new RuntimeException("Invalid request - POST requires at least 1 content stream");
      }
    }

    @Override
    protected MirroredConfigSetRequest getThis() {
      return this;
    }

    @Override
    public SolrParams getParams() {
      return params;
    }

    public void setParams(ModifiableSolrParams params) {
      this.params = params;
    }

    @Override
    public Collection<ContentStream> getContentStreams() {
      return contentStreams;
    }

    @Override
    protected ConfigSetAdminResponse createResponse(SolrClient client) {
      return new ConfigSetAdminResponse();
    }
  }

  public static class ExposedByteArrayContentStream extends ContentStreamBase {
    private final byte[] bytes;

    public ExposedByteArrayContentStream(byte[] bytes, String source, String contentType) {
      this.bytes = bytes;

      this.contentType = contentType;
      name = source;
      size = (long) bytes.length;
      sourceInfo = source;
    }

    public byte[] byteArray() {
      return bytes;
    }

    @Override
    public InputStream getStream() throws IOException {
      return new ByteArrayInputStream(bytes);
    }

    @Override
    public String toString() {
      return "contentType="
          + contentType
          + ", name="
          + name
          + ", sourceInfo="
          + sourceInfo
          + ", size="
          + size;
    }

    public static ExposedByteArrayContentStream of(ContentStream cs) throws IOException {
      if (cs instanceof ExposedByteArrayContentStream) {
        return (ExposedByteArrayContentStream) cs;
      }
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      cs.getStream().transferTo(baos);
      ExposedByteArrayContentStream res =
          new ExposedByteArrayContentStream(
              baos.toByteArray(), cs.getSourceInfo(), cs.getContentType());
      res.setName(cs.getName());
      return res;
    }
  }

  private final SolrRequest<T> solrRequest;
  private final Type type;

  // Attempts counter for processing the request
  private int attempt = 1;

  // Timestamp to track when this request was first written. This should be used to track the
  // replication lag.
  private long submitTimeNanos = 0;

  public MirroredSolrRequest(final SolrRequest<T> solrRequest) {
    this(Type.UPDATE, 1, solrRequest, System.nanoTime());
  }

  public MirroredSolrRequest(final Type type, final SolrRequest<T> solrRequest) {
    this(type, 1, solrRequest);
  }

  public MirroredSolrRequest(final Type type, final int attempt, final SolrRequest<T> solrRequest) {
    this(type, attempt, solrRequest, System.nanoTime());
  }

  public MirroredSolrRequest(
      final Type type,
      final int attempt,
      final SolrRequest<T> solrRequest,
      final long submitTimeNanos) {
    if (solrRequest == null) {
      throw new NullPointerException("solrRequest cannot be null");
    }
    this.type = type;
    this.attempt = attempt;
    this.solrRequest = solrRequest;
    this.submitTimeNanos = submitTimeNanos;
  }

  public MirroredSolrRequest(final int attempt, final long submitTimeNanos) {
    this.type = Type.UPDATE;
    this.attempt = attempt;
    this.submitTimeNanos = submitTimeNanos;
    solrRequest = null;
  }

  public int getAttempt() {
    return attempt;
  }

  public void setAttempt(final int attempt) {
    this.attempt = attempt;
  }

  public SolrRequest<T> getSolrRequest() {
    return solrRequest;
  }

  public long getSubmitTimeNanos() {
    return submitTimeNanos;
  }

  public void setSubmitTimeNanos(final long submitTimeNanos) {
    this.submitTimeNanos = submitTimeNanos;
  }

  public Type getType() {
    return type;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (!(o instanceof MirroredSolrRequest)) return false;

    final MirroredSolrRequest<?> that = (MirroredSolrRequest) o;

    return Objects.equals(solrRequest, that.solrRequest);
  }

  public static void setParams(SolrRequest<?> request, ModifiableSolrParams params) {
    if (request instanceof MirroredAdminRequest) {
      ((MirroredAdminRequest) request).setParams(params);
    } else if (request instanceof UpdateRequest) {
      ((UpdateRequest) request).setParams(params);
    } else {
      throw new UnsupportedOperationException("Can't setParams on request " + request);
    }
  }

  @Override
  public int hashCode() {
    return solrRequest.hashCode();
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(getClass().getSimpleName() + "{type=");
    sb.append(type.toString());
    sb.append(", method=" + solrRequest.getMethod());
    sb.append(", params=" + solrRequest.getParams());
    if (solrRequest instanceof UpdateRequest) {
      UpdateRequest req = (UpdateRequest) solrRequest;
      sb.append(", add=" + (req.getDocuments() != null ? req.getDocuments().size() : "0"));
      sb.append(", del=" + (req.getDeleteByIdMap() != null ? req.getDeleteByIdMap().size() : "0"));
      sb.append(", dbq=" + (req.getDeleteQuery() != null ? req.getDeleteQuery().size() : "0"));
    }
    sb.append(", attempt=" + attempt);
    sb.append(", submitTimeNanos=" + submitTimeNanos);
    sb.append('}');
    return sb.toString();
  }
}
