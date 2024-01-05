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
package org.apache.solr.handler.admin.api;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.inject.Inject;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;
import org.apache.solr.client.api.endpoint.MergeIndexesApi;
import org.apache.solr.client.api.model.MergeIndexesRequestBody;
import org.apache.solr.client.api.model.SolrJerseyResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.CachingDirectoryFactory;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.DirectoryFactory;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.admin.CoreAdminHandler;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.security.PermissionNameProvider;
import org.apache.solr.update.MergeIndexesCommand;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.update.processor.UpdateRequestProcessorChain;
import org.apache.solr.util.RefCounted;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of V2 API interface {@link MergeIndexesApi} for merging one or more indexes to
 * another index.
 *
 * @see MergeIndexesApi
 * @see MergeIndexesRequestBody
 * @see CoreAdminAPIBase
 */
public class MergeIndexes extends CoreAdminAPIBase implements MergeIndexesApi {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Inject
  public MergeIndexes(
      CoreContainer coreContainer,
      CoreAdminHandler.CoreAdminAsyncTracker coreAdminAsyncTracker,
      SolrQueryRequest req,
      SolrQueryResponse rsp) {
    super(coreContainer, coreAdminAsyncTracker, req, rsp);
  }

  @Override
  @PermissionName(PermissionNameProvider.Name.CORE_EDIT_PERM)
  public SolrJerseyResponse mergeIndexes(String coreName, MergeIndexesRequestBody requestBody)
      throws Exception {
    ensureRequiredParameterProvided("coreName", coreName);
    SolrJerseyResponse response = instantiateJerseyResponse(SolrJerseyResponse.class);

    return handlePotentiallyAsynchronousTask(
        response,
        coreName,
        requestBody.async,
        "merge-indices",
        () -> {
          try {
            SolrCore core = coreContainer.getCore(coreName);
            SolrQueryRequest wrappedReq = null;
            if (core == null) return response;

            List<SolrCore> sourceCores = new ArrayList<>();
            List<RefCounted<SolrIndexSearcher>> searchers = new ArrayList<>();
            // stores readers created from indexDir param values
            List<DirectoryReader> readersToBeClosed = new ArrayList<>();
            Map<Directory, Boolean> dirsToBeReleased = new HashMap<>();

            try {
              var dirNames =
                  Optional.ofNullable(requestBody.indexDirs).orElseGet(() -> new ArrayList<>());
              if (dirNames.isEmpty()) {
                var sources =
                    Optional.ofNullable(requestBody.srcCores).orElseGet(() -> new ArrayList<>());
                if (sources.isEmpty())
                  throw new SolrException(
                      SolrException.ErrorCode.BAD_REQUEST,
                      "At least one indexDir or srcCore must be specified");
                sources.stream()
                    .forEach(
                        src -> {
                          String source = src;
                          SolrCore srcCore = coreContainer.getCore(source);
                          if (srcCore == null)
                            throw new SolrException(
                                SolrException.ErrorCode.BAD_REQUEST,
                                "Core: " + source + " does not exist");
                          sourceCores.add(srcCore);
                        });
              } else {
                // Validate each 'indexDir' input as valid
                dirNames.stream()
                    .forEach(
                        indexDir -> core.getCoreContainer().assertPathAllowed(Paths.get(indexDir)));
                DirectoryFactory dirFactory = core.getDirectoryFactory();
                dirNames.stream()
                    .forEach(
                        dir -> {
                          boolean markAsDone = false;
                          if (dirFactory instanceof CachingDirectoryFactory) {
                            if (!((CachingDirectoryFactory) dirFactory)
                                .getLivePaths()
                                .contains(dir)) {
                              markAsDone = true;
                            }
                          }
                          try {
                            Directory dirTemp =
                                dirFactory.get(
                                    dir,
                                    DirectoryFactory.DirContext.DEFAULT,
                                    core.getSolrConfig().indexConfig.lockType);
                            dirsToBeReleased.put(dirTemp, markAsDone);
                            // TODO: why doesn't this use the IR factory? what is going on here?
                            readersToBeClosed.add(DirectoryReader.open(dirTemp));
                          } catch (IOException e) {
                            throw new RuntimeException(e);
                          }
                        });
              }

              List<DirectoryReader> readers = null;
              if (readersToBeClosed.size() > 0) {
                readers = readersToBeClosed;
              } else {
                readers = new ArrayList<>();
                for (SolrCore solrCore : sourceCores) {
                  // record the searchers so that we can decref
                  RefCounted<SolrIndexSearcher> searcher = solrCore.getSearcher();
                  searchers.add(searcher);
                  readers.add(searcher.get().getRawReader());
                }
              }

              UpdateRequestProcessorChain processorChain =
                  core.getUpdateProcessingChain(requestBody.updateChain);
              wrappedReq = new LocalSolrQueryRequest(core, req.getParams());
              UpdateRequestProcessor processor = processorChain.createProcessor(wrappedReq, rsp);
              processor.processMergeIndexes(new MergeIndexesCommand(readers, req));
            } catch (Exception e) {
              // log and rethrow so that if the finally fails we don't lose the original problem
              log.error("ERROR executing merge:", e);
              throw e;
            } finally {
              for (RefCounted<SolrIndexSearcher> searcher : searchers) {
                if (searcher != null) searcher.decref();
              }
              for (SolrCore solrCore : sourceCores) {
                if (solrCore != null) solrCore.close();
              }
              IOUtils.closeWhileHandlingException(readersToBeClosed);
              Set<Map.Entry<Directory, Boolean>> entries = dirsToBeReleased.entrySet();
              for (Map.Entry<Directory, Boolean> entry : entries) {
                DirectoryFactory dirFactory = core.getDirectoryFactory();
                Directory dir = entry.getKey();
                boolean markAsDone = entry.getValue();
                if (markAsDone) {
                  dirFactory.doneWithDirectory(dir);
                }
                dirFactory.release(dir);
              }
              if (wrappedReq != null) wrappedReq.close();
              core.close();
            }
            return response;
          } catch (SolrException exp) {
            throw exp;
          } catch (Exception e) {
            throw new SolrException(
                SolrException.ErrorCode.SERVER_ERROR,
                "Failed to Merge Indexes=" + coreName + " because " + e,
                e);
          }
        });
  }
}
