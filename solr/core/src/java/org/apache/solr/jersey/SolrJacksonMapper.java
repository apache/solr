/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.jersey;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import jakarta.ws.rs.ext.ContextResolver;
import jakarta.ws.rs.ext.Provider;
import java.io.IOException;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.util.ByteArrayUtf8CharSequence;
import org.apache.solr.common.util.NamedList;

/** Customizes the ObjectMapper settings used for serialization/deserialization in Jersey */
@SuppressWarnings("rawtypes")
@Provider
public class SolrJacksonMapper implements ContextResolver<ObjectMapper> {

  private static final ObjectMapper objectMapper = createObjectMapper();

  @Override
  public ObjectMapper getContext(Class<?> type) {
    return objectMapper;
  }

  public static ObjectMapper getObjectMapper() {
    return objectMapper;
  }

  private static ObjectMapper createObjectMapper() {
    final SimpleModule customTypeModule = new SimpleModule();
    customTypeModule.addSerializer(new NamedListSerializer(NamedList.class));
    customTypeModule.addSerializer(new SolrDocumentListSerializer(SolrDocumentList.class));
    customTypeModule.addSerializer(
        new ByteArrayUtf8CharSequenceSerializer(ByteArrayUtf8CharSequence.class));
    customTypeModule.addSerializer(
        new IndexableFieldSerializer(org.apache.lucene.index.IndexableField.class));

    return new ObjectMapper()
        // TODO Should failOnUnknown=false be made available on a "permissive" object mapper instead
        // of the "main" one?
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        .setSerializationInclusion(JsonInclude.Include.NON_NULL)
        .registerModule(customTypeModule);
  }

  public static class NamedListSerializer extends StdSerializer<NamedList> {

    public NamedListSerializer() {
      this(null);
    }

    public NamedListSerializer(Class<NamedList> nlClazz) {
      super(nlClazz);
    }

    @Override
    public void serialize(NamedList value, JsonGenerator gen, SerializerProvider provider)
        throws IOException {
      gen.writeObject(value.asShallowMap());
    }
  }

  /**
   * Serializes {@link SolrDocumentList} to the standard Solr JSON format used by {@code
   * JSONResponseWriter}: {@code {"numFound":N,"start":0,"numFoundExact":true,"docs":[...]}}.
   *
   * <p>This is needed so that JAX-RS endpoints that delegate to a legacy {@link
   * org.apache.solr.handler.RequestHandlerBase} and copy handler results into a {@link
   * org.apache.solr.client.api.model.GetDocumentsResponse} can be serialized correctly by Jackson
   * without falling back to the legacy {@code JSONResponseWriter} path.
   */
  public static class SolrDocumentListSerializer extends StdSerializer<SolrDocumentList> {

    public SolrDocumentListSerializer() {
      this(null);
    }

    public SolrDocumentListSerializer(Class<SolrDocumentList> clazz) {
      super(clazz);
    }

    @Override
    public void serialize(SolrDocumentList value, JsonGenerator gen, SerializerProvider provider)
        throws IOException {
      gen.writeStartObject();
      gen.writeNumberField("numFound", value.getNumFound());
      gen.writeNumberField("start", value.getStart());
      if (value.getNumFoundExact() != null) {
        gen.writeBooleanField("numFoundExact", value.getNumFoundExact());
      }
      if (value.getMaxScore() != null) {
        gen.writeNumberField("maxScore", value.getMaxScore());
      }
      gen.writeArrayFieldStart("docs");
      for (SolrDocument doc : value) {
        // SolrDocument implements Map<String, Object>; Jackson serializes it as a JSON object.
        // ByteArrayUtf8CharSequence field values are handled by
        // ByteArrayUtf8CharSequenceSerializer.
        gen.writeObject(doc);
      }
      gen.writeEndArray();
      gen.writeEndObject();
    }
  }

  /**
   * Serializes {@link ByteArrayUtf8CharSequence} — Solr's internal efficient representation of
   * stored string field values — as a plain JSON string via {@code toString()}.
   *
   * <p>Without this serializer, Jackson treats {@code ByteArrayUtf8CharSequence} as a bean and
   * produces an object like {@code {"charSequenceValue":"1"}} instead of the string {@code "1"}.
   */
  public static class ByteArrayUtf8CharSequenceSerializer
      extends StdSerializer<ByteArrayUtf8CharSequence> {

    public ByteArrayUtf8CharSequenceSerializer() {
      this(null);
    }

    public ByteArrayUtf8CharSequenceSerializer(Class<ByteArrayUtf8CharSequence> clazz) {
      super(clazz);
    }

    @Override
    public void serialize(
        ByteArrayUtf8CharSequence value, JsonGenerator gen, SerializerProvider provider)
        throws IOException {
      gen.writeString(value.toString());
    }
  }

  /**
   * Serializes Lucene {@link org.apache.lucene.index.IndexableField} objects to their actual
   * values.
   *
   * <p>When documents are retrieved from the index (not from the transaction log), {@link
   * SolrDocument} may contain {@link org.apache.lucene.index.IndexableField} objects. This
   * serializer extracts the actual field value (numeric, string, or binary) for JSON output.
   */
  public static class IndexableFieldSerializer
      extends StdSerializer<org.apache.lucene.index.IndexableField> {

    public IndexableFieldSerializer() {
      this(null);
    }

    public IndexableFieldSerializer(Class<org.apache.lucene.index.IndexableField> clazz) {
      super(clazz);
    }

    @Override
    public void serialize(
        org.apache.lucene.index.IndexableField value,
        JsonGenerator gen,
        SerializerProvider provider)
        throws IOException {
      // Try numeric value first
      Number numericValue = value.numericValue();
      if (numericValue != null) {
        gen.writeNumber(numericValue.toString());
        return;
      }
      // Fall back to string value
      String stringValue = value.stringValue();
      if (stringValue != null) {
        gen.writeString(stringValue);
        return;
      }
      // If neither, try binary value
      org.apache.lucene.util.BytesRef binaryValue = value.binaryValue();
      if (binaryValue != null) {
        gen.writeString(binaryValue.utf8ToString());
        return;
      }
      // If all else fails, write null
      gen.writeNull();
    }
  }
}
