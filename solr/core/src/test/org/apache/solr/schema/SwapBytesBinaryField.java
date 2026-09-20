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

import java.nio.ByteBuffer;
import java.util.List;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;

/**
 * Custom binary field that internaly reverses the btes, but if all the I/O layers of Solr work
 * correctly, clients should never know
 */
public final class SwapBytesBinaryField extends BinaryField
    implements FieldType.ExternalizeStoredValuesAsObjects {

  public static byte[] copyAndReverse(final byte[] array, final int offset, final int length) {
    final byte[] result = new byte[length];
    for (int i = 0; i < length; i++) {
      result[i] = array[offset + length - 1 - i];
    }
    return result;
  }

  public static ByteBuffer copyAndReverse(final ByteBuffer in) {
    return ByteBuffer.wrap(
        copyAndReverse(in.array(), in.arrayOffset() + in.position(), in.remaining()));
  }

  public static BytesRef copyAndReverse(final BytesRef in) {
    return new BytesRef(copyAndReverse(in.bytes, in.offset, in.length));
  }

  @Override
  public ByteBuffer toObject(IndexableField f) {
    return copyAndReverse(super.toObject(f));
  }

  @Override
  public Object toObject(SchemaField sf, BytesRef term) {
    return copyAndReverse(term).bytes;
  }

  /**
   * This is kludgy, but since BinaryField doesn't let us override "getBytesRef(Object)", this is
   * the simplest place for us to reverse things after super has generated fields with binary values
   * for us.
   */
  @Override
  public List<IndexableField> createFields(SchemaField field, Object val) {
    final List<IndexableField> results = super.createFields(field, val);
    for (IndexableField indexable : results) {
      if (indexable instanceof Field f) {
        if (null != f.binaryValue()) {
          f.setBytesValue(copyAndReverse(f.binaryValue()));
        }
      } else {
        throw new RuntimeException(
            "WTF: test is broken by unexpected type of IndexableField from super");
      }
    }
    return results;
  }

  @Override
  public Object toNativeType(Object val) {
    Object result = super.toNativeType(val);
    if (result instanceof ByteBuffer originalBuf) {
      result = copyAndReverse(originalBuf);
    }
    return result;
  }
}
