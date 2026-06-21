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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.solr.client.solrj.request.JavaBinUpdateRequestCodec;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ZooKeeperException;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.common.util.*;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Update handler which uses the JavaBin format
 *
 * @see org.apache.solr.client.solrj.request.JavaBinUpdateRequestCodec
 * @see org.apache.solr.common.util.JavaBinCodec
 */
public class JavabinLoader extends ContentStreamLoader {
  final ContentStreamLoader contentStreamLoader;

  public JavabinLoader() {
    this.contentStreamLoader = this;
  }

  public JavabinLoader(ContentStreamLoader contentStreamLoader) {
    super();
    this.contentStreamLoader = contentStreamLoader;
  }

  @Override
  public void load(SolrQueryRequest req, SolrQueryResponse rsp, ContentStream stream, UpdateRequestProcessor processor) throws Exception {
    InputStream is = stream.getStream();
    parseAndLoadDocs(req, rsp, is, processor);
  }

  private void parseAndLoadDocs(final SolrQueryRequest req, SolrQueryResponse rsp, InputStream stream, final UpdateRequestProcessor processor)
      throws IOException {
    if (req.getParams().getBool("multistream", false)) {
      handleMultiStream(req, rsp, stream, processor);
      return;
    }
    UpdateRequest update = null;
    JavaBinUpdateRequestCodec.StreamingUpdateHandler handler = new MyStreamingUpdateHandler(req, processor);

    JavaBinUpdateRequestCodec javaBin = new JavaBinUpdateRequestCodec();

    for (; ; ) {
      try {
        update = javaBin.unmarshal(stream, handler);
      } catch (EOFException e) {
        break; // this is expected
      }
      if (update.getDeleteByIdMap() != null || update.getDeleteQuery() != null) {
        delete(req, update, processor);
      }
    }
  }

  private void handleMultiStream(SolrQueryRequest req, SolrQueryResponse rsp, InputStream stream, UpdateRequestProcessor processor)
      throws IOException {
    SolrParams old = req.getParams();
    try (JavaBinCodec jbc = new JavaBinCodec() {
      SolrParams params;
      @Override
      public List<Object> readIterator(JavaBinInputStream fis) throws IOException {
        while (true) {
          Object o = readVal(fis);
          if (o == END_OBJ) break;
          if (o instanceof NamedList) {
            params = ((NamedList) o).toSolrParams();
          } else {
            try {
              if (o instanceof byte[]) {
                if (params != null) req.setParams(params);
                byte[] buf = (byte[]) o;
                contentStreamLoader.load(req, rsp, new ContentStreamBase.ByteArrayStream(buf, null), processor);
              } else {
                throw new RuntimeException("unsupported type ");
              }
            } catch (Exception e) {
              throw new RuntimeException(e);
            } finally {
              params = null;
              req.setParams(old);
            }
          }
        }
        return Collections.emptyList();
      }

    }) {
      jbc.unmarshal(FastInputStream.wrap(stream));
    }
  }

  private static AddUpdateCommand getAddCommand(SolrQueryRequest req, SolrParams params) {
    AddUpdateCommand templateAdd = AddUpdateCommand.THREAD_LOCAL_AddUpdateCommand.get();
    templateAdd.clear();
    templateAdd.setReq(req);
    templateAdd.overwrite = params.getBool(UpdateParams.OVERWRITE, true);
    templateAdd.commitWithin = params.getInt(UpdateParams.COMMIT_WITHIN, -1);
    return templateAdd;
  }

  private static void delete(SolrQueryRequest req, UpdateRequest update, UpdateRequestProcessor processor) throws IOException {
    SolrParams params = update.getParams();
    DeleteUpdateCommand delcmd = new DeleteUpdateCommand(req);
    if(params != null) {
      delcmd.commitWithin = params.getInt(UpdateParams.COMMIT_WITHIN, -1);
    }
    
    if(update.getDeleteByIdMap() != null) {
      Set<Entry<String,Map<String,Object>>> entries = update.getDeleteByIdMap().entrySet();
      for (Entry<String,Map<String,Object>> e : entries) {
        delcmd.id = e.getKey();
        Map<String,Object> map = e.getValue();
        if (map != null) {
          Long version = (Long) map.get("ver");
          if (version != null) {
            delcmd.setVersion(version);
          }
        }
        if (map != null) {
          String route = (String) map.get(ShardParams._ROUTE_);
          if (route != null) {
            delcmd.setRoute(route);
          }
        }
        processor.processDelete(delcmd);
        delcmd.clear();
      }
    }
    
    if(update.getDeleteQuery() != null) {
      for (String s : update.getDeleteQuery()) {
        delcmd.query = s;
        processor.processDelete(delcmd);
      }
    }
  }

  private static class MyStreamingUpdateHandler implements JavaBinUpdateRequestCodec.StreamingUpdateHandler {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final SolrQueryRequest req;
    private final UpdateRequestProcessor processor;

    public MyStreamingUpdateHandler(SolrQueryRequest req, UpdateRequestProcessor processor) {
      this.req = req;
      this.processor = processor;
    }

    @Override
    public void update(SolrInputDocument document, UpdateRequest updateRequest, Integer commitWithin, Boolean overwrite) {
      log.info("update SolrInputDocument={}", document);
      if (document == null) {
        return;
      }

      AddUpdateCommand addCmd = getAddCommand(req, updateRequest.getParams());

      addCmd.solrDoc = document;
      if (commitWithin != null) {
        addCmd.commitWithin = commitWithin;
      }
      if (overwrite != null) {
        addCmd.overwrite = overwrite;
      }

      if (updateRequest.isLastDocInBatch()) {
        // this is a hint to downstream code that indicates we've sent the last doc in a batch
        addCmd.isLastDocInBatch = true;
      }

      try {
        processor.processAdd(addCmd);
      } catch (ZooKeeperException | IOException e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "ERROR adding document " + addCmd.getPrintableId(), e);
      }
    }
  }
}
