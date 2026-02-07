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
package org.apache.solr.handler.admin;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.util.Date;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.security.AuthorizationContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This handler returns core level info. See {@link org.apache.solr.handler.admin.SystemInfoHandler}
 */
public class CoreInfoHandler extends RequestHandlerBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Override
  public String getDescription() {
    return "Get Core Info";
  }

  @Override
  public Category getCategory() {
    return Category.ADMIN;
  }

  @Override
  public Name getPermissionName(AuthorizationContext request) {
    return Name.CONFIG_READ_PERM;
  }

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    rsp.setHttpCaching(false);
    SolrCore core = req.getCore();

    rsp.add("core", getCoreInfo(core, req.getSchema()));
  }

  private SimpleOrderedMap<Object> getCoreInfo(SolrCore core, IndexSchema schema) {
    SimpleOrderedMap<Object> info = new SimpleOrderedMap<>();

    info.add("schema", schema != null ? schema.getSchemaName() : "no schema!");

    // Now
    info.add("now", new Date());

    // Start Time
    info.add("start", core.getStartTimeStamp());

    // Solr Home
    SimpleOrderedMap<Object> dirs = new SimpleOrderedMap<>();
    dirs.add("cwd", Path.of(System.getProperty("user.dir")).toAbsolutePath().toString());
    dirs.add("instance", core.getInstancePath().toString());
    try {
      dirs.add("data", core.getDirectoryFactory().normalize(core.getDataDir()));
    } catch (IOException e) {
      log.warn("Problem getting the normalized data directory path", e);
    }
    dirs.add("dirimpl", core.getDirectoryFactory().getClass().getName());
    try {
      dirs.add("index", core.getDirectoryFactory().normalize(core.getIndexDir()));
    } catch (IOException e) {
      log.warn("Problem getting the normalized index directory path", e);
    }
    info.add("directory", dirs);
    return info;
  }
}
