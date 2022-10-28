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
package org.apache.solr.handler;

import static org.apache.solr.common.params.CommonParams.NAME;

import java.io.IOException;
import java.io.Reader;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.IOUtils;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.AuthorizationContext;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DumpRequestHandler extends RequestHandlerBase implements SolrCoreAware {
  private SolrCore solrCore;
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Override
  @SuppressWarnings({"unchecked"})
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws IOException {
    // Show params
    rsp.add("params", req.getParams().toNamedList());
    String[] parts = req.getParams().getParams("urlTemplateValues");
    if (parts != null && parts.length > 0) {
      Map<String, String> map = new LinkedHashMap<>();
      rsp.getValues().add("urlTemplateValues", map);
      for (String part : parts) {
        map.put(part, req.getPathTemplateValues().get(part));
      }
    }

    String[] returnParams = req.getParams().getParams("param");
    if (returnParams != null) {
      @SuppressWarnings({"unchecked"})
      NamedList<Object> params = (NamedList<Object>) rsp.getValues().get("params");
      for (String returnParam : returnParams) {
        String[] vals = req.getParams().getParams(returnParam);
        if (vals != null) {
          if (vals.length == 1) {
            params.add(returnParam, vals[0]);
          } else {
            params.add(returnParam, vals);
          }
        }
      }
    }

    if (req.getParams().getBool("getdefaults", false)) {
      NamedList<?> def = (NamedList<?>) initArgs.get(PluginInfo.DEFAULTS);
      rsp.add("getdefaults", def);
    }

    if (req.getParams().getBool("initArgs", false)) {
      rsp.add("initArgs", initArgs);
    }

    // Write the streams...
    if (req.getContentStreams() != null) {
      ArrayList<NamedList<Object>> streams = new ArrayList<>();
      // Cycle through each stream
      for (ContentStream content : req.getContentStreams()) {
        NamedList<Object> stream = new SimpleOrderedMap<>();
        stream.add(NAME, content.getName());
        stream.add("sourceInfo", content.getSourceInfo());
        stream.add("size", content.getSize());
        stream.add("contentType", content.getContentType());
        Reader reader = content.getReader();
        try {
          stream.add("stream", IOUtils.toString(reader));
        } finally {
          reader.close();
        }
        streams.add(stream);
      }
      rsp.add("streams", streams);
    }

    rsp.add("context", req.getContext());
  }

  //////////////////////// SolrInfoMBeans methods //////////////////////

  @Override
  public String getDescription() {
    return "Dump handler (debug)";
  }

  @Override
  public SolrRequestHandler getSubHandler(String subPath) {
    if (subpaths != null && subpaths.contains(subPath)) return this;
    return null;
  }

  private List<String> subpaths;

  @Override
  public void init(NamedList<?> args) {
    super.init(args);
    if (args != null) {
      @SuppressWarnings("unchecked")
      NamedList<String> nl = (NamedList<String>) args.get(PluginInfo.DEFAULTS);
      if (nl != null) subpaths = nl.getAll("subpath");
    }
  }

  @Override
  public Name getPermissionName(AuthorizationContext request) {
    if (solrCore != null && solrCore.getSolrConfig().isEnableRemoteStreams()) {
      log.warn(
          "Dump request handler requires config-read permission when remote streams are enabled");
      return Name.CONFIG_READ_PERM;
    } else {
      return Name.ALL;
    }
  }

  @Override
  public void inform(SolrCore core) {
    this.solrCore = core;
  }
}
