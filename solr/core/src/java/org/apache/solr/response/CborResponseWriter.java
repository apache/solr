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

import com.fasterxml.jackson.dataformat.cbor.CBORFactory;
import com.fasterxml.jackson.dataformat.cbor.CBORGenerator;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import org.apache.solr.client.solrj.impl.BinaryResponseParser;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;

/**
 * A response writer impl that can write results in CBOR (cbor.io) format when wt=cbor. It uses the
 * jackson library to write the stream out
 */
public class CborResponseWriter extends BinaryResponseWriter {
  final CBORFactory cborFactory;
  final CBORFactory cborFactoryCompact;

  public CborResponseWriter() {
    cborFactoryCompact = CBORFactory.builder().enable(CBORGenerator.Feature.STRINGREF).build();
    cborFactory = CBORFactory.builder().build();
  }

  @Override
  public void write(OutputStream out, SolrQueryRequest req, SolrQueryResponse response)
      throws IOException {
    boolean useStringRef = req.getParams().getBool("string_ref", true);
    WriterImpl writer =
        new WriterImpl(useStringRef ? cborFactoryCompact : cborFactory, out, req, response);
    writer.writeResponse();
    writer.gen.flush();
  }

  @Override
  public String getContentType(SolrQueryRequest request, SolrQueryResponse response) {
    return BinaryResponseParser.BINARY_CONTENT_TYPE;
  }

  static class WriterImpl extends JSONWriter {

    final CBORGenerator gen;

    public WriterImpl(
        CBORFactory factory, OutputStream out, SolrQueryRequest req, SolrQueryResponse rsp)
        throws IOException {
      super(null, req, rsp);
      gen = factory.createGenerator(out);
    }

    @Override
    public void writeResponse() throws IOException {
      super.writeNamedList(null, rsp.getValues());
    }

    @SuppressWarnings("rawtypes")
    public void write(NamedList nl) throws IOException {
      super.writeNamedList(null, nl);
    }

    @Override
    public void writeNumber(String name, Number val) throws IOException {
      if (val instanceof Integer) {
        gen.writeNumber(val.intValue());
      } else if (val instanceof Long) {
        gen.writeNumber(val.longValue());
      } else if (val instanceof Float) {
        gen.writeNumber(val.floatValue());
      } else if (val instanceof Double) {
        gen.writeNumber(val.doubleValue());
      } else if (val instanceof Short) {
        gen.writeNumber(val.shortValue());
      } else if (val instanceof Byte) {
        gen.writeNumber(val.byteValue());
      } else if (val instanceof BigInteger) {
        gen.writeNumber((BigInteger) val);
      } else if (val instanceof BigDecimal) {
        gen.writeNumber((BigDecimal) val);
      } else {
        gen.writeString(val.getClass().getName() + ':' + val.toString());
        // default... for debugging only
      }
    }

    @Override
    public void writeBool(String name, Boolean val) throws IOException {
      gen.writeBoolean(val);
    }

    @Override
    public void writeNull(String name) throws IOException {
      gen.writeNull();
    }

    @Override
    public void writeStr(String name, String val, boolean needsEscaping) throws IOException {
      gen.writeString(val);
    }

    @Override
    public void writeLong(String name, long val) throws IOException {
      gen.writeNumber(val);
    }

    @Override
    public void writeInt(String name, int val) throws IOException {
      gen.writeNumber(val);
    }

    @Override
    public void writeBool(String name, boolean val) throws IOException {
      gen.writeBoolean(val);
    }

    @Override
    public void writeFloat(String name, float val) throws IOException {
      gen.writeNumber(val);
    }

    @Override
    public void writeArrayCloser() throws IOException {
      gen.writeEndArray();
    }

    @Override
    public void writeArraySeparator() {
      // do nothing
    }

    @Override
    public void writeArrayOpener(int size) throws IOException, IllegalArgumentException {
      gen.writeStartArray();
    }

    @Override
    public void writeMapCloser() throws IOException {
      gen.writeEndObject();
    }

    @Override
    public void writeKey(String fname, boolean needsEscaping) throws IOException {
      gen.writeFieldName(fname);
    }

    @Override
    public void writeMapOpener(int size) throws IOException, IllegalArgumentException {
      gen.writeStartObject();
    }

    @Override
    public void writeByteArr(String name, byte[] buf, int offset, int len) throws IOException {
      gen.writeBinary(buf, offset, len);
    }

    @Override
    public void writeMapSeparator() throws IOException {
      // do nothing
    }

    @Override
    public void indent() throws IOException {
      // do nothing
    }

    @Override
    public void indent(int lev) throws IOException {
      // do nothing
    }

    @Override
    public int incLevel() {
      return 0;
    }

    @Override
    public int decLevel() {
      return 0;
    }
  }
}
