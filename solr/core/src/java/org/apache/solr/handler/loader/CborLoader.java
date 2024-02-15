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
package org.apache.solr.handler.loader;

import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.cbor.CBORFactory;
import com.fasterxml.jackson.dataformat.cbor.CBORGenerator;
import com.fasterxml.jackson.dataformat.cbor.CBORParser;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.processor.UpdateRequestProcessor;

/**
 * This class can load a single document or a stream of documents in CBOR format this is equivalent
 * of loading a single json documet or an array of json documents
 */
public class CborLoader {
  final CBORFactory cborFactory;
  private final Consumer<SolrInputDocument> sink;

  public CborLoader(CBORFactory cborFactory, Consumer<SolrInputDocument> sink) {
    this.cborFactory =
        cborFactory == null
            ? CBORFactory.builder().enable(CBORGenerator.Feature.STRINGREF).build()
            : cborFactory;
    this.sink = sink;
  }

  public void stream(InputStream is) throws IOException {
    ObjectMapper mapper = new ObjectMapper(new CBORFactory());
    try (CBORParser parser = (CBORParser) mapper.getFactory().createParser(is)) {
      JsonToken t;
      while ((t = parser.nextToken()) != null) {

        if (t == JsonToken.START_ARRAY) {
          // this is an array of docs
          t = parser.nextToken();
          if (t == JsonToken.START_OBJECT) {
            handleDoc(parser);
          }

        } else if (t == JsonToken.START_OBJECT) {
          // this is just a single doc
          handleDoc(parser);
        }
      }
    }
  }

  private void handleDoc(CBORParser p) throws IOException {
    SolrInputDocument doc = new SolrInputDocument();
    for (; ; ) {
      JsonToken t = p.nextToken();
      if (t == JsonToken.END_OBJECT) {
        if (!doc.isEmpty()) {
          sink.accept(doc);
        }
        return;
      }
      String name;
      if (t == JsonToken.FIELD_NAME) {
        name = p.getCurrentName();
        t = p.nextToken();
        if (t == JsonToken.START_ARRAY) {

          List<Object> l = new ArrayList<>();
          for (; ; ) {
            t = p.nextToken();
            if (t == JsonToken.END_ARRAY) break;
            else {
              l.add(readVal(t, p));
            }
          }
          if (!l.isEmpty()) {
            doc.addField(name, l);
          }

        } else {
          doc.addField(name, readVal(t, p));
        }
      }
    }
  }

  private Object readVal(JsonToken t, CBORParser p) throws IOException {
    if (t == JsonToken.VALUE_NULL) {
      return null;
    }
    if (t == JsonToken.VALUE_STRING) {
      return p.getValueAsString();
    }
    if (t == JsonToken.VALUE_TRUE) {
      return Boolean.TRUE;
    }
    if (t == JsonToken.VALUE_FALSE) {
      return Boolean.FALSE;
    }
    if (t == JsonToken.VALUE_NUMBER_INT || t == JsonToken.VALUE_NUMBER_FLOAT) {
      return p.getNumberValue();
    }
    if (t == JsonToken.START_OBJECT) {
      Object[] returnVal = new Object[1];
      new CborLoader(cborFactory, d -> returnVal[0] = d).handleDoc(p);
      return returnVal[0];
    }
    throw new RuntimeException("Unknown type :" + t);
  }

  public static ContentStreamLoader createLoader(SolrParams p) {
    CBORFactory factory = CBORFactory.builder().enable(CBORGenerator.Feature.STRINGREF).build();
    return new ContentStreamLoader() {
      @Override
      public void load(
          SolrQueryRequest req,
          SolrQueryResponse rsp,
          ContentStream stream,
          UpdateRequestProcessor processor)
          throws IOException {
        int commitWithin = req.getParams().getInt(UpdateParams.COMMIT_WITHIN, -1);
        boolean overwrite = req.getParams().getBool(UpdateParams.OVERWRITE, true);
        new CborLoader(
                factory,
                doc -> {
                  AddUpdateCommand add = new AddUpdateCommand(req);
                  add.commitWithin = commitWithin;
                  add.solrDoc = doc;
                  add.overwrite = overwrite;
                  try {
                    processor.processAdd(add);
                  } catch (IOException e) {
                    throw new RuntimeException(e);
                  }
                })
            .stream(stream.getStream());
      }
    }.init(p);
  }
}
