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
package org.apache.solr.schema;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.response.TextResponseWriter;

/**
 * Custom binary field that is always Base64 stringified with external clients, using a special
 * prefix
 */
public final class StrBinaryField extends BinaryField {
  public static final String PREFIX = "CUSTOMPRE_";

  @Override
  public void write(TextResponseWriter writer, String name, IndexableField f) throws IOException {
    writer.writeStr(name, toExternal(f), false);
  }

  @Override
  public String toExternal(IndexableField f) {
    return PREFIX + super.toExternal(f);
  }

  @Override
  public Object toObject(SchemaField sf, BytesRef term) {
    return PREFIX
        + Base64.getEncoder()
            .encodeToString(Arrays.copyOfRange(term.bytes, term.offset, term.offset + term.length));
  }

  @Override
  public List<IndexableField> createFields(SchemaField field, Object val) {
    if (val instanceof String valStr) {
      if (valStr.startsWith(PREFIX)) {
        return super.createFields(field, valStr.substring(PREFIX.length()));
      }
      throw new RuntimeException(
          field.getName() + " values must be strings PREFIXED with " + PREFIX + "; got: " + valStr);
    }
    throw new RuntimeException(
        field.getName()
            + " values must be STRINGS starting with "
            + PREFIX
            + "; got: "
            + val.getClass());
  }

  @Override
  public Object toNativeType(Object val) {
    Object result = super.toNativeType(val);
    if (result instanceof ByteBuffer buf) {
      // Kludge because super doesn't give us access to it's method...
      result =
          new String(
              Base64.getEncoder()
                  .encode(
                      ByteBuffer.wrap(
                              buf.array(),
                              buf.arrayOffset() + buf.position(),
                              buf.limit() - buf.position())
                          .array()),
              StandardCharsets.ISO_8859_1);
    }
    return PREFIX + result.toString();
  }
}
