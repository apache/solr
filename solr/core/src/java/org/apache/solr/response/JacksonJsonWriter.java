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

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.util.DefaultIndenter;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import org.apache.solr.common.PushWriter;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.request.SolrQueryRequest;

/** A JSON ResponseWriter that uses jackson. */
public class JacksonJsonWriter implements TextQueryResponseWriter {

  protected final JsonFactory jsonfactory;
  protected static final DefaultPrettyPrinter pretty =
      new DefaultPrettyPrinter()
          .withoutSpacesInObjectEntries()
          .withArrayIndenter(DefaultPrettyPrinter.NopIndenter.instance)
          .withObjectIndenter(new DefaultIndenter().withLinefeed("\n"));

  public JacksonJsonWriter() {
    super();
    jsonfactory = new JsonFactory();
  }

  // let's also implement the binary version since Jackson supports that (probably faster)
  @Override
  public void write(
      OutputStream out, SolrQueryRequest request, SolrQueryResponse response, String contentType)
      throws IOException {
    out = new NonFlushingStream(out);
    // resolve the encoding
    final String charSet = ContentStreamBase.getCharsetFromContentType(contentType);
    JsonEncoding jsonEncoding;
    if (charSet != null) {
      assert JsonEncoding.values().length < 10; // fast to iterate
      jsonEncoding =
          Arrays.stream(JsonEncoding.values())
              .filter(e -> e.getJavaName().equalsIgnoreCase(charSet))
              .findAny()
              .orElseThrow(() -> new UnsupportedEncodingException(charSet));
    } else {
      jsonEncoding = JsonEncoding.UTF8;
    }

    var sw = new WriterImpl(request, response, jsonfactory.createGenerator(out, jsonEncoding));
    sw.writeResponse();
    sw.close();
  }

  @Override
  public void write(Writer writer, SolrQueryRequest request, SolrQueryResponse response)
      throws IOException {
    var sw = new WriterImpl(request, response, jsonfactory.createGenerator(writer));
    sw.writeResponse();
    sw.close();
  }

  public PushWriter getWriter(
      OutputStream out, SolrQueryRequest request, SolrQueryResponse response) throws IOException {
    return new WriterImpl(request, response, jsonfactory.createGenerator(out, JsonEncoding.UTF8));
  }

  @Override
  public String getContentType(SolrQueryRequest request, SolrQueryResponse response) {
    return JSONResponseWriter.CONTENT_TYPE_JSON_UTF8;
  }

  // So we extend JSONWriter and override the relevant methods

  public static class WriterImpl extends JSONWriter {

    protected JsonGenerator gen;

    public WriterImpl(SolrQueryRequest req, SolrQueryResponse rsp, JsonGenerator generator) {
      super(null, req, rsp);
      gen = generator;
      if (doIndent) {
        gen.setPrettyPrinter(pretty.createInstance());
      }
    }

    @Override
    public void writeResponse() throws IOException {
      if (wrapperFunction != null) {
        writeStrRaw(null, wrapperFunction + "(");
      }
      super.writeNamedList(null, rsp.getValues());
      if (wrapperFunction != null) {
        writeStrRaw(null, ")");
      }
      gen.close();
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
    public void writeMapSeparator() {
      // do nothing
    }

    @Override
    public void writeMapOpener(int size) throws IOException, IllegalArgumentException {
      gen.writeStartObject();
    }

    @Override
    public void writeKey(String fname, boolean needsEscaping) throws IOException {
      gen.writeFieldName(fname);
    }

    @Override
    public void writeByteArr(String name, byte[] buf, int offset, int len) throws IOException {
      gen.writeBinary(buf, offset, len);
    }

    @Override
    public void setLevel(int level) {
      // do nothing
    }

    @Override
    public int level() {
      return 0;
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

    @Override
    public void close() throws IOException {
      gen.close();
    }

    @Override
    public void writeStrRaw(String name, String val) throws IOException {
      gen.writeRawValue(val);
    }

    @Override
    public void writeDate(String name, String val) throws IOException {
      gen.writeString(val);
    }
  }
}
