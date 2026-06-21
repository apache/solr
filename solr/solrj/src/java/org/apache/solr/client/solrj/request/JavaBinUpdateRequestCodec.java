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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;

import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.params.CommonParams.CHILDDOC;
import static org.apache.solr.common.util.ByteArrayUtf8CharSequence.convertCharSeq;

/**
 * Provides methods for marshalling an UpdateRequest to a NamedList which can be serialized in the javabin format and
 * vice versa.
 *
 *
 * @see org.apache.solr.common.util.JavaBinCodec
 * @since solr 1.4
 */
public class JavaBinUpdateRequestCodec {
  private AtomicBoolean readStringAsCharSeq = new AtomicBoolean(false);

  private final AtomicBoolean seenOuterMostDocIterator = new AtomicBoolean(false);

  public JavaBinUpdateRequestCodec setReadStringAsCharSeq(boolean flag) {
    this.readStringAsCharSeq.set(flag);
    return this;

  }

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final AtomicBoolean WARNED_ABOUT_INDEX_TIME_BOOSTS = new AtomicBoolean();

  /**
   * Converts an UpdateRequest to a NamedList which can be serialized to the given OutputStream in the javabin format
   *
   * @param updateRequest the UpdateRequest to be written out
   * @param os            the OutputStream to which the request is to be written
   *
   * @throws IOException in case of an exception during marshalling or writing to the stream
   */
  public static void marshal(UpdateRequest updateRequest, OutputStream os) throws IOException {
    NamedList nl = new NamedList();
    NamedList params = solrParamsToNamedList(updateRequest.getParams());
    if (updateRequest.getCommitWithin() != -1) {
      params.add("commitWithin", updateRequest.getCommitWithin());
    }
    Iterator<SolrInputDocument> docIter = null;

    if(updateRequest.getDocIterator() != null){
      docIter = updateRequest.getDocIterator();
    }

    Map<SolrInputDocument,Map<String,Object>> docMap = updateRequest.getDocumentsMap();

    nl.add("params", params);// 0: params
    if (updateRequest.getDeleteByIdMap() != null) {
      nl.add("delByIdMap", updateRequest.getDeleteByIdMap());
    }
    nl.add("delByQ", updateRequest.getDeleteQuery());

    if (docMap != null) {
      nl.add("docsMap", docMap.entrySet().iterator());
    } else {
      if (updateRequest.getDocuments() != null) {
        docIter = updateRequest.getDocuments().iterator();
      }
      nl.add("docs", docIter);
    }
    try (JavaBinCodec codec = new JavaBinCodec()) {
      codec.marshal(nl, os);
    }
  }

  public static void marshal(UpdateRequest updateRequest, FastOutputStream os) throws IOException {
    NamedList nl = new NamedList();
    NamedList params = solrParamsToNamedList(updateRequest.getParams());
    if (updateRequest.getCommitWithin() != -1) {
      params.add("commitWithin", updateRequest.getCommitWithin());
    }
    Iterator<SolrInputDocument> docIter = null;

    if(updateRequest.getDocIterator() != null){
      docIter = updateRequest.getDocIterator();
    }

    Map<SolrInputDocument,Map<String,Object>> docMap = updateRequest.getDocumentsMap();

    nl.add("params", params);// 0: params
    if (updateRequest.getDeleteByIdMap() != null) {
      nl.add("delByIdMap", updateRequest.getDeleteByIdMap());
    }
    nl.add("delByQ", updateRequest.getDeleteQuery());

    if (docMap != null) {
      nl.add("docsMap", docMap.entrySet().iterator());
    } else {
      if (updateRequest.getDocuments() != null) {
        docIter = updateRequest.getDocuments().iterator();
      }
      nl.add("docs", docIter);
    }
    try (JavaBinCodec codec = new JavaBinCodec()) {
      codec.marshal(nl, os);
    }
  }

  /**
   * Reads a NamedList from the given InputStream, converts it into a SolrInputDocument and passes it to the given
   * StreamingUpdateHandler
   *
   * @param is      the InputStream from which to read
   * @param handler an instance of StreamingUpdateHandler to which SolrInputDocuments are streamed one by one
   *
   * @return the UpdateRequest
   *
   * @throws IOException in case of an exception while reading from the input stream or unmarshalling
   */
  public UpdateRequest unmarshal(InputStream is, final StreamingUpdateHandler handler) throws IOException {
    final UpdateRequest updateRequest = new UpdateRequest();
  //  List<List<NamedList>> doclist;
  //  List<Entry<SolrInputDocument,Map<Object,Object>>>  docMap;
    List<String> delById = null;
    Map<String,Map<String,Object>> delByIdMap = null;
    List<String> delByQ = null;
    final NamedList[] namedList = new NamedList[1];
    try (JavaBinCodec codec = new StreamingCodec(namedList, updateRequest, handler, seenOuterMostDocIterator, readStringAsCharSeq)) {
      codec.unmarshal(FastInputStream.wrap(is));
    }

    // NOTE: if the update request contains only delete commands the params
    // must be loaded now
    if(updateRequest.getParams()==null && namedList[0] != null) {
      NamedList params = (NamedList) namedList[0].get("params");
      if(params!=null) {
        updateRequest.setParams(new ModifiableSolrParams(params.toSolrParams()));
      }
    }
    if (namedList[0] != null) {
      delById = (List<String>) namedList[0].get("delById");
      delByIdMap = (Map<String,Map<String,Object>>) namedList[0].get("delByIdMap");
      delByQ = (List<String>) namedList[0].get("delByQ");
      //   doclist = (List) namedList[0].get("docs");
      Object docsMapObj = namedList[0].get("docsMap");
    }
//
//    if (docsMapObj instanceof Map) {//SOLR-5762
//      docMap =  new ArrayList(((Map)docsMapObj).entrySet());
//    } else {
//      docMap = (List<Entry<SolrInputDocument, Map<Object, Object>>>) docsMapObj;
//    }


    // we don't add any docs, because they were already processed
    // deletes are handled later, and must be passed back on the UpdateRequest

    if (delById != null) {
      for (String s : delById) {
        updateRequest.deleteById(s);
      }
    }
    if (delByIdMap != null) {
      for (Map.Entry<String,Map<String,Object>> entry : delByIdMap.entrySet()) {
        Map<String,Object> params = entry.getValue();
        if (params != null) {
          Long version = (Long) params.get(UpdateRequest.VER);
          if (params.containsKey(ShardParams._ROUTE_)) {
            updateRequest.deleteById(entry.getKey(), (String) params.get(ShardParams._ROUTE_));
          } else {
            updateRequest.deleteById(entry.getKey(), version);
          }
        } else {
          updateRequest.deleteById(entry.getKey());
        }

      }
    }
    if (delByQ != null) {
      for (String s : delByQ) {
        updateRequest.deleteByQuery(s);
      }
    }

    return updateRequest;
  }


  private static NamedList solrParamsToNamedList(SolrParams params) {
    if (params == null) return new NamedList();
    return params.toNamedList();
  }

  public interface StreamingUpdateHandler {
    void update(SolrInputDocument document, UpdateRequest req, Integer commitWithin, Boolean override);
  }

  public static class MaskCharSequenceSolrInputDoc extends SolrInputDocument {
    public MaskCharSequenceSolrInputDoc(Map<String, SolrInputField> fields) {
      super(fields);
    }

    @Override
    public Object getFieldValue(String name) {
      return convertCharSeq(super.getFieldValue(name));
    }

  }

  static class StreamingCodec extends JavaBinCodec {

    private final NamedList[] namedList;
    private final UpdateRequest updateRequest;
    private final StreamingUpdateHandler handler;
    private final AtomicBoolean seenOuterMostDocIterator;
    private final AtomicBoolean readStringAsCharSeq;

    public StreamingCodec(NamedList[] namedList, UpdateRequest updateRequest, StreamingUpdateHandler handler, AtomicBoolean readStringAsCharSeq, AtomicBoolean seenOuterMostDocIterator) {
      this.namedList = namedList;
      this.updateRequest = updateRequest;
      this.handler = handler;
      this.seenOuterMostDocIterator = seenOuterMostDocIterator;
      this.seenOuterMostDocIterator.set(false);
      this.readStringAsCharSeq = readStringAsCharSeq;
    }

    @Override
    protected SolrInputDocument createSolrInputDocument(int sz) {
      return new SolrInputDocument(new Object2ObjectLinkedOpenHashMap<>(sz, 0.5f));
    }

    @Override
    public NamedList readNamedList(JavaBinInputStream dis, int sz) throws IOException {

      NamedList nl = new NamedList();
      if (namedList[0] == null) {
        namedList[0] = nl;
      }
      for (int i = 0; i < sz; i++) {
        String name = (String) readVal(dis);
        Object val = readVal(dis);
        nl.add(name, val);
      }
      return nl;
    }

    private static SolrInputDocument listToSolrInputDocument(List<NamedList> namedList) {
      SolrInputDocument doc = new SolrInputDocument();
      final int size = namedList.size();
      for (int i = 0; i < size; i++) {
        NamedList nl = namedList.get(i);
        doc.addField((String) nl.getVal(0),
              nl.getVal(1));
      }
      return doc;
    }

    @Override
    public List readIterator(JavaBinInputStream fis) throws IOException {
      // default behavior for reading any regular Iterator in the stream
      if (seenOuterMostDocIterator.get()) return super.readIterator(fis);

      // special treatment for first outermost Iterator
      // (the list of documents)
      seenOuterMostDocIterator.set(true);
      return readOuterMostDocIterator(fis);
    }


    private List readOuterMostDocIterator(JavaBinInputStream fis) throws IOException {
      if(namedList[0] == null) namedList[0] = new NamedList();
      NamedList params = (NamedList) namedList[0].get("params");
      if (params == null) params = new NamedList();
      updateRequest.setParams(new ModifiableSolrParams(params.toSolrParams()));
      if (handler == null) return super.readIterator(fis);
      Integer commitWithin = null;
      Boolean overwrite = null;
      Object o = null;
      super.readStringAsCharSeq.set(readStringAsCharSeq.get());
      try {
        while (true) {
          if (o == null) {
            o = readVal(fis);
          }

          log.info("read request javabin object as {} {}", o.getClass().getName(), o);
          if (o == END_OBJ) {
            break;
          }

          SolrInputDocument sdoc = null;
          if (o instanceof List) {
            sdoc = listToSolrInputDocument((List<NamedList>) o);
          } else if (o instanceof NamedList) {
            UpdateRequest req = new UpdateRequest();
            req.setParams(new ModifiableSolrParams(((NamedList) o).toSolrParams()));
            handler.update(null, req, null, null);
          } else if (o instanceof SolrInputDocument) {
            sdoc = (SolrInputDocument) o;
          } else if (o instanceof Map) {
            sdoc = convertMapToSolrInputDoc((Map) o);
          } else if (o instanceof Map.Entry) {
            Object key = ((Entry) o).getKey();
            log.info("request Map.Entry key object is {} {}", key.getClass().getName(), key);
            // NOTE: SolrInputDocument implements Map, so check it FIRST. Otherwise a parent doc
            // with anonymous child documents would be run through convertMapToSolrInputDoc (which
            // copies only fields), silently dropping its child documents and breaking block/nested
            // indexing in the distributed update path.
            if (key instanceof SolrInputDocument) {
              sdoc = (SolrInputDocument) key;
            } else if (key instanceof Map) {
              sdoc = convertMapToSolrInputDoc((Map) key);
            }

            Map p = (Map) ((Entry) o).getValue();
            log.info("request Map.Entry value object is {} {}",p == null ? "null" : p.getClass().getName(), p);
            if (p != null) {
              commitWithin = (Integer) p.get(UpdateRequest.COMMIT_WITHIN);
              overwrite = (Boolean) p.get(UpdateRequest.OVERWRITE);
            }
          }

          // peek at the next object to see if we're at the end
          o = readVal(fis);
          if (o == END_OBJ) {
            // indicate that we've hit the last doc in the batch, used to enable optimizations when doing replication
            updateRequest.lastDocInBatch();
          }

          handler.update(sdoc, updateRequest, commitWithin, overwrite);
        }
        return Collections.EMPTY_LIST;
      } finally {
        super.readStringAsCharSeq.set(false);

      }
    }

    private SolrInputDocument convertMapToSolrInputDoc(Map m) {
      SolrInputDocument result = createSolrInputDocument(m.size());
      m.forEach((k, v) -> {
        if (CHILDDOC.equals(k.toString())) {
          if (v instanceof List) {
            List list = (List) v;
            for (Object o : list) {
              if (o instanceof Map) {
                result.addChildDocument(convertMapToSolrInputDoc((Map) o));
              }
            }
          } else if (v instanceof Map) {
            result.addChildDocument(convertMapToSolrInputDoc((Map) v));
          }
        } else {
          if (v instanceof SolrInputField) {
            // Preserve the original value (which may be a Collection for multi-valued fields).
            // Calling toString() here collapsed a multi-valued List like [4, 2] into the literal
            // string "[4, 2]", which then failed to parse for non-string field types.
            result.addField(k.toString(), ((SolrInputField) v).getValue());
          } else {
            result.addField(k.toString(), v);
          }
        }
      });
      return result;
    }

  }
}
