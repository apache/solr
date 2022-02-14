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
package org.apache.solr.response;

import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.search.ReturnFields;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * Utility class that delegates to another {@link TextResponseWriter}, but converts normal write requests
 * into "raw" requests that write field values directly to the delegate {@link TextResponseWriter}'s backing writer.
 */
class RawShimTextResponseWriter extends TextResponseWriter {

  private final TextResponseWriter backing;

  RawShimTextResponseWriter(TextResponseWriter backing) {
    super(null, false);
    this.backing = backing;
  }

  // convert non-raw to raw. These are the reason this class exists (see class javadocs)
  @Override
  public void writeStr(String name, String val, boolean needsEscaping) throws IOException {
    backing.writeStrRaw(name, val);
  }

  @Override
  public void writeArray(String name, Iterator<?> val, boolean raw) throws IOException {
    backing.writeArray(name, val, true);
  }

  // Other stuff; just no-op delegation
  @Override
  public void writeStartDocumentList(String name, long start, int size, long numFound, Float maxScore, Boolean numFoundExact) throws IOException {
    backing.writeStartDocumentList(name, start, size, numFound, maxScore, numFoundExact);
  }

  @Override
  public void writeSolrDocument(String name, SolrDocument doc, ReturnFields fields, int idx) throws IOException {
    backing.writeSolrDocument(name, doc, fields, idx);
  }

  @Override
  public void writeEndDocumentList() throws IOException {
    backing.writeEndDocumentList();
  }

  @Override
  public void writeMap(String name, Map<?, ?> val, boolean excludeOuter, boolean isFirstVal) throws IOException {
    backing.writeMap(name, val, excludeOuter, isFirstVal);
  }

  @Override
  public void writeNull(String name) throws IOException {
    backing.writeNull(name);
  }

  @Override
  public void writeInt(String name, String val) throws IOException {
    backing.writeInt(name, val);
  }

  @Override
  public void writeLong(String name, String val) throws IOException {
    backing.writeLong(name, val);
  }

  @Override
  public void writeBool(String name, String val) throws IOException {
    backing.writeBool(name, val);
  }

  @Override
  public void writeFloat(String name, String val) throws IOException {
    backing.writeFloat(name, val);
  }

  @Override
  public void writeDouble(String name, String val) throws IOException {
    backing.writeDouble(name, val);
  }

  @Override
  public void writeDate(String name, String val) throws IOException {
    backing.writeDate(name, val);
  }

  @Override
  public void writeNamedList(String name, NamedList<?> val) throws IOException {
    backing.writeNamedList(name, val);
  }
}
