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

import static org.apache.solr.handler.ReplicationHandler.ERR_STATUS;
import static org.apache.solr.handler.ReplicationHandler.OK_STATUS;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.solr.api.JerseyResource;
import org.apache.solr.core.DirectoryFactory;
import org.apache.solr.core.IndexDeletionPolicyWrapper;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.ReplicationHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A common parent for "replication" (i.e. replication-level) APIs. */
public abstract class ReplicationAPIBase extends JerseyResource {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  protected final SolrCore solrCore;
  protected final SolrQueryRequest solrQueryRequest;
  protected final SolrQueryResponse solrQueryResponse;

  public ReplicationAPIBase(
      SolrCore solrCore, SolrQueryRequest solrQueryRequest, SolrQueryResponse solrQueryResponse) {
    this.solrCore = solrCore;
    this.solrQueryRequest = solrQueryRequest;
    this.solrQueryResponse = solrQueryResponse;
  }

  protected CoreReplicationAPI.IndexVersionResponse doFetchIndexVersion() throws IOException {
    ReplicationHandler replicationHandler =
        (ReplicationHandler) solrCore.getRequestHandler(ReplicationHandler.PATH);
    return replicationHandler.getIndexVersionResponse();
  }

  protected CoreReplicationAPI.FileListResponse doFetchFileList(long generation) {
    ReplicationHandler replicationHandler =
        (ReplicationHandler) solrCore.getRequestHandler(ReplicationHandler.PATH);
    return getFileList(generation, replicationHandler);
  }

  protected CoreReplicationAPI.FileListResponse getFileList(
      long generation, ReplicationHandler replicationHandler) {
    final IndexDeletionPolicyWrapper delPol = solrCore.getDeletionPolicy();
    final CoreReplicationAPI.FileListResponse filesResponse =
        new CoreReplicationAPI.FileListResponse();

    IndexCommit commit = null;
    try {
      if (generation == -1) {
        commit = delPol.getAndSaveLatestCommit();
        if (null == commit) {
          filesResponse.fileList = Collections.emptyList();
          return filesResponse;
        }
      } else {
        try {
          commit = delPol.getAndSaveCommitPoint(generation);
        } catch (IllegalStateException ignored) {
          /* handle this below the same way we handle a return value of null... */
        }
        if (null == commit) {
          // The gen they asked for either doesn't exist or has already been deleted
          reportErrorOnResponse(filesResponse, "invalid index generation", null);
          return filesResponse;
        }
      }
      assert null != commit;

      List<CoreReplicationAPI.FileMetaData> result = new ArrayList<>();
      Directory dir = null;
      try {
        dir =
            solrCore
                .getDirectoryFactory()
                .get(
                    solrCore.getNewIndexDir(),
                    DirectoryFactory.DirContext.DEFAULT,
                    solrCore.getSolrConfig().indexConfig.lockType);
        SegmentInfos infos = SegmentInfos.readCommit(dir, commit.getSegmentsFileName());
        for (SegmentCommitInfo commitInfo : infos) {
          for (String file : commitInfo.files()) {
            CoreReplicationAPI.FileMetaData metaData = new CoreReplicationAPI.FileMetaData();
            metaData.name = file;
            metaData.size = dir.fileLength(file);

            try (final IndexInput in = dir.openInput(file, IOContext.READONCE)) {
              try {
                long checksum = CodecUtil.retrieveChecksum(in);
                metaData.checksum = checksum;
              } catch (Exception e) {
                // TODO Should this trigger a larger error?
                log.warn("Could not read checksum from index file: {}", file, e);
              }
            }
            result.add(metaData);
          }
        }

        // add the segments_N file
        CoreReplicationAPI.FileMetaData fileMetaData = new CoreReplicationAPI.FileMetaData();
        fileMetaData.name = infos.getSegmentsFileName();
        fileMetaData.size = dir.fileLength(infos.getSegmentsFileName());
        if (infos.getId() != null) {
          try (final IndexInput in =
              dir.openInput(infos.getSegmentsFileName(), IOContext.READONCE)) {
            try {
              fileMetaData.checksum = CodecUtil.retrieveChecksum(in);
            } catch (Exception e) {
              // TODO Should this trigger a larger error?
              log.warn(
                  "Could not read checksum from index file: {}", infos.getSegmentsFileName(), e);
            }
          }
        }
        result.add(fileMetaData);
      } catch (IOException e) {
        log.error(
            "Unable to get file names for indexCommit generation: {}", commit.getGeneration(), e);
        reportErrorOnResponse(
            filesResponse, "unable to get file names for given index generation", e);
        return filesResponse;
      } finally {
        if (dir != null) {
          try {
            solrCore.getDirectoryFactory().release(dir);
          } catch (IOException e) {
            log.error("Could not release directory after fetching file list", e);
          }
        }
      }
      filesResponse.fileList = new ArrayList<>(result);

      if (replicationHandler.getConfFileNameAlias().size() < 1
          || solrCore.getCoreContainer().isZooKeeperAware()) return filesResponse;
      String includeConfFiles = replicationHandler.getIncludeConfFiles();
      log.debug("Adding config files to list: {}", includeConfFiles);
      // if configuration files need to be included get their details
      filesResponse.confFiles =
          new ArrayList<>(
              replicationHandler.getConfFileInfoFromCache(
                  replicationHandler.getConfFileNameAlias(),
                  replicationHandler.getConfFileInfoCache()));
      filesResponse.status = OK_STATUS;

    } finally {
      if (null != commit) {
        // before releasing the save on our commit point, set a short reserve duration since
        // the main reason remote nodes will ask for the file list is because they are preparing to
        // replicate from us...
        delPol.setReserveDuration(
            commit.getGeneration(), replicationHandler.getReserveCommitDuration());
        delPol.releaseCommitPoint(commit);
      }
    }
    return filesResponse;
  }

  private void reportErrorOnResponse(
      CoreReplicationAPI.FileListResponse fileListResponse, String message, Exception e) {
    fileListResponse.status = ERR_STATUS;
    fileListResponse.message = message;
    if (e != null) {
      fileListResponse.exception = e;
    }
  }
}
