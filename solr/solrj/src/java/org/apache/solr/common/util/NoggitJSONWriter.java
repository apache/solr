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

import java.io.IOException;
import java.util.function.BiConsumer;
import org.apache.solr.common.IteratorWriter;
import org.apache.solr.common.MapWriter;
import org.noggit.CharArr;
import org.noggit.JSONWriter;

/** Serialize JSON using noggit */
public class NoggitJSONWriter extends JSONWriter {

  public NoggitJSONWriter(CharArr out, int indentSize) {
    super(out, indentSize);
  }

  @Override
  @SuppressWarnings("rawtypes")
  public void handleUnknownClass(Object o) {
    // avoid materializing MapWriter / IteratorWriter to Map / List
    // instead serialize them directly
    if (o instanceof MapWriter) {
      MapWriter mapWriter = (MapWriter) o;
      startObject();

      mapWriter._forEachEntry(
          new BiConsumer() {
            boolean first = true;

            @Override
            public void accept(Object k, Object v) {
              if (first) {
                first = false;
              } else {
                writeValueSeparator();
              }
              indent();
              writeString(k.toString());
              writeNameSeparator();
              write(v);
            }
          });
      endObject();
    } else if (o instanceof IteratorWriter) {
      IteratorWriter iteratorWriter = (IteratorWriter) o;
      startArray();
      try {
        iteratorWriter.writeIter(
            new IteratorWriter.ItemWriter() {
              boolean first = true;

              @Override
              public IteratorWriter.ItemWriter add(Object o) {
                if (first) {
                  first = false;
                } else {
                  writeValueSeparator();
                }
                indent();
                write(o);
                return this;
              }
            });
      } catch (IOException e) {
        throw new RuntimeException("this should never happen", e);
      }
      endArray();
    } else {
      super.handleUnknownClass(o);
    }
  }
}
