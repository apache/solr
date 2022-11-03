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
  public void write(Object o) {
    if (o instanceof MapWriter) {
      MapWriter mapWriter = (MapWriter) o;
      startObject();
      try {
        mapWriter.writeMap(entryWriter());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      endObject();
    } else if (o instanceof IteratorWriter) {
      IteratorWriter iteratorWriter = (IteratorWriter) o;
      startArray();
      try {
        iteratorWriter.writeIter(itemWriter());
      } catch (IOException e) {
        throw new RuntimeException("this should never happen", e);
      }
      endArray();
    } else if (o instanceof MapWriter.StringValue) {
      super.write(o.toString());
    } else {
      super.write(o);
    }
  }

  private IteratorWriter.ItemWriter itemWriter() {
    return new IteratorWriter.ItemWriter() {
      private boolean first = true;

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
    };
  }

  private MapWriter.EntryWriter entryWriter() {
    return new MapWriter.EntryWriter() {
      private boolean first = true;

      @Override
      public MapWriter.EntryWriter put(CharSequence k, Object v) {
        if (first) {
          first = false;
        } else {
          writeValueSeparator();
        }
        indent();
        writeString(k.toString());
        writeNameSeparator();
        write(v);
        return this;
      }
    };
  }
}
