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
package org.apache.solr.client.solrj.request;

import static org.apache.solr.common.params.CommonParams.CHILDDOC;
import static org.apache.solr.common.util.ByteArrayUtf8CharSequence.convertCharSeq;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.util.CollectionUtil;
import org.apache.solr.common.util.DataInputInputStream;
import org.apache.solr.common.util.JavaBinCodec;
import org.apache.solr.common.util.NamedList;

/**
 * Provides methods for marshalling an UpdateRequest to a NamedList which can be serialized in the
 * javabin format and vice versa.
 *
 * @see org.apache.solr.common.util.JavaBinCodec
 * @since solr 1.4
 */
public class JavaBinUpdateRequestCodec {

  /**
   * Converts an UpdateRequest to a NamedList which can be serialized to the given OutputStream in
   * the javabin format
   *
   * @param updateRequest the UpdateRequest to be written out
   * @param os the OutputStream to which the request is to be written
   * @throws IOException in case of an exception during marshalling or writing to the stream
   */
  public void marshal(UpdateRequest updateRequest, OutputStream os) throws IOException {
    NamedList<Object> nl = new NamedList<>();

    NamedList<Object> params = updateRequest.getParams().toNamedList();
    if (updateRequest.getCommitWithin() != -1) {
      params.add("commitWithin", updateRequest.getCommitWithin());
    }
    nl.add("params", params); // 0: params

    if (updateRequest.getDeleteByIdMap() != null) {
      nl.add("delByIdMap", updateRequest.getDeleteByIdMap());
    }

    nl.add("delByQ", updateRequest.getDeleteQuery());

    Map<SolrInputDocument, Map<String, Object>> docMap = updateRequest.getDocumentsMap();
    if (docMap != null) {
      nl.add("docsMap", docMap.entrySet().iterator()); // of Map.Entry
    } else {
      Iterator<SolrInputDocument> docIter;
      if (updateRequest.getDocuments() != null) {
        docIter = updateRequest.getDocuments().iterator();
      } else {
        docIter = updateRequest.getDocIterator();
      }
      nl.add("docs", docIter);
    }

    try (JavaBinCodec codec = new JavaBinCodec()) {
      codec.marshal(nl, os);
    }
  }

  /**
   * Reads a NamedList from the given InputStream, converts it into a SolrInputDocument and passes
   * it to the given StreamingUpdateHandler
   *
   * @param is the InputStream from which to read
   * @param handler an instance of StreamingUpdateHandler to which SolrInputDocuments are streamed
   *     one by one
   * @return the UpdateRequest
   * @throws IOException in case of an exception while reading from the input stream or
   *     unmarshalling
   */
  @SuppressWarnings({"unchecked"})
  public UpdateRequest unmarshal(InputStream is, final StreamingUpdateHandler handler)
      throws IOException {
    final NamedList<Object> namedList;

    // process documents:

    // reads documents, sending to handler.  Other data is in NamedList
    try (var codec = new StreamingCodec(handler)) {
      namedList = codec.unmarshal(is);
    }

    // process deletes:

    final UpdateRequest updateRequest = new UpdateRequest();
    {
      NamedList<?> params = (NamedList<?>) namedList.get("params");
      if (params != null) {
        updateRequest.setParams(ModifiableSolrParams.of(params.toSolrParams()));
      }
    }

    for (String s : (List<String>) namedList.getOrDefault("delById", List.of())) {
      updateRequest.deleteById(s);
    }

    for (var entry :
        ((Map<String, Map<String, Object>>) namedList.getOrDefault("delByIdMap", Map.of()))
            .entrySet()) {
      Map<String, Object> params = entry.getValue();
      if (params != null) {
        Long version = (Long) params.get(UpdateRequest.VER);
        if (params.containsKey(ShardParams._ROUTE_)) {
          updateRequest.deleteById(
              entry.getKey(), (String) params.get(ShardParams._ROUTE_), version);
        } else {
          updateRequest.deleteById(entry.getKey(), version);
        }
      } else {
        updateRequest.deleteById(entry.getKey());
      }
    }

    for (String s : (List<String>) namedList.getOrDefault("delByQ", List.of())) {
      updateRequest.deleteByQuery(s);
    }

    return updateRequest;
  }

  public interface StreamingUpdateHandler {
    void update(
        SolrInputDocument document, UpdateRequest req, Integer commitWithin, Boolean override);
  }

  static class MaskCharSequenceSolrInputDoc extends SolrInputDocument {
    public MaskCharSequenceSolrInputDoc(Map<String, SolrInputField> fields) {
      super(fields);
    }

    @Override
    public Object getFieldValue(String name) {
      return convertCharSeq(super.getFieldValue(name));
    }
  }

  static class StreamingCodec extends JavaBinCodec {

    private NamedList<Object> resultNamedList;
    private final StreamingUpdateHandler handler;
    // NOTE: this only works because this is an anonymous inner class
    // which will only ever be used on a single stream -- if this class
    // is ever refactored, this will not work.
    private boolean seenOuterMostDocIterator = false;

    StreamingCodec(StreamingUpdateHandler handler) {
      this.handler = handler;
    }

    @Override
    public NamedList<Object> unmarshal(InputStream is) throws IOException {
      super.unmarshal(is);
      return resultNamedList;
    }

    @Override
    protected SolrInputDocument createSolrInputDocument(int sz) {
      return new MaskCharSequenceSolrInputDoc(CollectionUtil.newLinkedHashMap(sz));
    }

    @Override
    public NamedList<Object> readNamedList(DataInputInputStream dis) throws IOException {
      int sz = readSize(dis);
      NamedList<Object> nl = new NamedList<>();
      if (this.resultNamedList == null) {
        this.resultNamedList = nl;
      }
      for (int i = 0; i < sz; i++) {
        String name = (String) readVal(dis);
        Object val = readVal(dis);
        nl.add(name, val);
      }
      return nl;
    }

    @Override
    public List<Object> readIterator(DataInputInputStream fis) throws IOException {
      // default behavior for reading any regular Iterator in the stream
      if (seenOuterMostDocIterator) return super.readIterator(fis);

      // special treatment for first outermost Iterator
      // (the list of documents)
      seenOuterMostDocIterator = true;
      readDocs(fis);
      return List.of(); // bogus; already processed
    }

    private void readDocs(DataInputInputStream fis) throws IOException {
      if (resultNamedList == null) resultNamedList = new NamedList<>();

      UpdateRequest updateRequest = new UpdateRequest();
      NamedList<?> params = (NamedList<?>) resultNamedList.get("params"); // always precedes docs
      if (params != null) {
        updateRequest.setParams(ModifiableSolrParams.of(params.toSolrParams()));
      }

      Object o = readVal(fis);
      while (o != END_OBJ) {
        Integer commitWithin = null;
        Boolean overwrite = null;

        SolrInputDocument sdoc;
        if (o instanceof Map.Entry) { // doc + options.  UpdateRequest "docsMap"
          @SuppressWarnings("unchecked")
          Map.Entry<SolrInputDocument, Map<?, ?>> entry =
              (Map.Entry<SolrInputDocument, Map<?, ?>>) o;
          sdoc = entry.getKey();
          Map<?, ?> p = entry.getValue();
          if (p != null) {
            commitWithin = (Integer) p.get(UpdateRequest.COMMIT_WITHIN);
            overwrite = (Boolean) p.get(UpdateRequest.OVERWRITE);
          }
        } else if (o instanceof SolrInputDocument d) { // doc.  UpdateRequest "docs""
          sdoc = d;
        } else if (o instanceof Map<?, ?> m) { // doc.  To imitate JSON style.  SOLR-13731
          sdoc = convertMapToSolrInputDoc(m);
        } else {
          throw new SolrException(ErrorCode.BAD_REQUEST, "Unexpected data type: " + o.getClass());
        }

        // peek at the next object to see if we're at the end
        o = readVal(fis);
        if (o == END_OBJ) {
          // indicate that we've hit the last doc in the batch, used to enable optimizations when
          // doing replication
          updateRequest.lastDocInBatch();
        }

        handler.update(sdoc, updateRequest, commitWithin, overwrite);
      }
    }

    private SolrInputDocument convertMapToSolrInputDoc(Map<?, ?> m) {
      SolrInputDocument result = createSolrInputDocument(m.size());
      m.forEach(
          (k, v) -> {
            if (CHILDDOC.equals(k.toString())) {
              if (v instanceof List<?> list) {
                for (Object o : list) {
                  if (o instanceof Map) {
                    result.addChildDocument(convertMapToSolrInputDoc((Map<?, ?>) o));
                  }
                }
              } else if (v instanceof Map) {
                result.addChildDocument(convertMapToSolrInputDoc((Map<?, ?>) v));
              }
            } else {
              result.addField(k.toString(), v);
            }
          });
      return result;
    }
  }
}
