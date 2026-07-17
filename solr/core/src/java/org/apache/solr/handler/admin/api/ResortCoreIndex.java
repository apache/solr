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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SlowCodecReaderWrapper;
import org.apache.lucene.index.SortingCodecReader;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.Sort;
import org.apache.lucene.store.Directory;
import org.apache.solr.client.api.endpoint.ResortCoreIndexApi;
import org.apache.solr.client.api.model.ResortCoreIndexRequestBody;
import org.apache.solr.client.api.model.ResortCoreIndexResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.DirectoryFactory;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.IndexFetcher;
import org.apache.solr.handler.admin.CoreAdminHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SortSpecParsing;
import org.apache.solr.util.RefCounted;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * V2 API implementation for re-sorting an existing core index (SOLR-12239).
 *
 * <p>Re-sorts a (possibly unsorted) core index into the target {@link Sort} using the LUCENE-9484
 * mechanism — each segment reader is wrapped in a {@link SortingCodecReader} and merged into a
 * fresh sort-configured {@link IndexWriter} via {@link IndexWriter#addIndexes(CodecReader...)} —
 * then swaps it in via {@code modifyIndexProps} and reopens the writer/searcher (mirroring
 * RestoreCore).
 *
 * <p>Not supported in SolrCloud mode. Indexes with child/nested documents are rejected.
 */
public class ResortCoreIndex extends CoreAdminAPIBase implements ResortCoreIndexApi {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public ResortCoreIndex(
      CoreContainer coreContainer,
      CoreAdminHandler.CoreAdminAsyncTracker coreAdminAsyncTracker,
      SolrQueryRequest req,
      SolrQueryResponse rsp) {
    super(coreContainer, coreAdminAsyncTracker, req, rsp);
  }

  @Override
  public boolean isExpensive() {
    return true;
  }

  @Override
  public ResortCoreIndexResponse resortCoreIndex(
      String coreName, ResortCoreIndexRequestBody requestBody) throws Exception {
    ensureRequiredParameterProvided("coreName", coreName);
    if (coreContainer.isZooKeeperAware()) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST, "resort is not supported in SolrCloud mode");
    }
    final ResortCoreIndexResponse response =
        instantiateJerseyResponse(ResortCoreIndexResponse.class);
    final String sortParam = requestBody == null ? null : requestBody.sort;
    final String async = requestBody == null ? null : requestBody.async;

    return handlePotentiallyAsynchronousTask(
        response,
        coreName,
        async,
        "resort-index",
        () -> {
          try (SolrCore core = coreContainer.getCore(coreName)) {
            if (core == null) {
              throw new SolrException(
                  SolrException.ErrorCode.BAD_REQUEST, "Core not found: " + coreName);
            }
            final Sort indexSort = resolveTargetSort(core, sortParam);
            assertNoChildDocs(core);
            resortAndSwap(core, indexSort);
            response.core = coreName;
            response.indexSort = indexSort.toString();
          } catch (Exception e) {
            throw new CoreAdminAPIBaseException(e);
          }
          return response;
        });
  }

  private Sort resolveTargetSort(SolrCore core, String sortParam) throws IOException {
    if (sortParam != null && !sortParam.isBlank()) {
      final Sort sort = SortSpecParsing.parseSortSpec(sortParam, core.getLatestSchema()).getSort();
      if (sort == null) {
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST,
            "Could not parse a usable index sort from sort=" + sortParam);
      }
      return sort;
    }
    // Fall back to the sort configured for the core: the directly configured <indexSort>
    // (preferred), or a SortingMergePolicy's sort (deprecated). This mirrors how the searcher
    // resolves the index sort, so the migration workflow "set <indexSort>, then RESORTINDEX" works.
    final String configuredSpec = core.getSolrConfig().indexConfig.indexSort;
    if (configuredSpec != null && !configuredSpec.isBlank()) {
      final Sort sort =
          SortSpecParsing.parseSortSpec(configuredSpec, core.getLatestSchema()).getSort();
      if (sort == null) {
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST,
            "Could not parse a usable index sort from the configured <indexSort>="
                + configuredSpec);
      }
      return sort;
    }
    final Sort mergePolicySort = core.getSolrCoreState().getMergePolicySort();
    if (mergePolicySort == null) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "No 'sort' parameter given and the core has no configured index sort "
              + "(via <indexSort> or a SortingMergePolicy) to fall back to");
    }
    return mergePolicySort;
  }

  private void assertNoChildDocs(SolrCore core) throws IOException {
    RefCounted<SolrIndexSearcher> ref = core.getSearcher();
    try {
      SolrIndexSearcher searcher = ref.get();
      if (!searcher.getSchema().isUsableForChildDocs()) {
        return;
      }
      String uniqueKeyField = searcher.getSchema().getUniqueKeyField().getName();
      for (LeafReaderContext leaf : searcher.getIndexReader().leaves()) {
        Terms rootTerms = leaf.reader().terms(IndexSchema.ROOT_FIELD_NAME);
        if (rootTerms == null) {
          continue;
        }
        long uniqueRootValues = rootTerms.size();
        Terms idTerms = leaf.reader().terms(uniqueKeyField);
        long uniqueIdValues = (idTerms != null) ? idTerms.size() : -1;
        if (uniqueRootValues == -1 || uniqueIdValues == -1 || uniqueRootValues < uniqueIdValues) {
          throw new SolrException(
              SolrException.ErrorCode.BAD_REQUEST,
              "resort does not support indexes containing child/nested documents. "
                  + "Consider reindexing your data from the original source.");
        }
      }
    } finally {
      ref.decref();
    }
  }

  private void resortAndSwap(SolrCore core, Sort indexSort) throws Exception {
    final String resortIndexName = "resort." + System.nanoTime();
    final String resortIndexPath = core.getDataDir() + resortIndexName;
    final String currentIndexPath = core.getIndexDir();
    final DirectoryFactory df = core.getDirectoryFactory();
    final String lockType = core.getSolrConfig().indexConfig.lockType;

    Directory currentDir = null;
    Directory resortDir = null;
    try {
      currentDir = df.get(currentIndexPath, DirectoryFactory.DirContext.DEFAULT, lockType);
      resortDir = df.get(resortIndexPath, DirectoryFactory.DirContext.DEFAULT, lockType);

      final IndexWriterConfig iwc = new IndexWriterConfig().setIndexSort(indexSort);
      // addIndexes rewrites every segment through this writer, so it must use the core's codec and
      // similarity; otherwise a per-field (e.g. SchemaCodecFactory) index would be silently
      // rewritten with Lucene defaults.
      iwc.setCodec(core.getCodec());
      iwc.setSimilarity(core.getLatestSchema().getSimilarity());
      // Mirror SolrIndexConfig: a child-doc-capable schema records a parent field, which the source
      // segments carry, so the re-sort writer must declare the same parent field or addIndexes
      // fails.
      if (core.getLatestSchema().isUsableForChildDocs()) {
        iwc.setParentField(IndexSchema.IS_ROOT_FIELD_NAME);
      }

      try (DirectoryReader reader = DirectoryReader.open(currentDir);
          IndexWriter resortWriter = new IndexWriter(resortDir, iwc)) {
        final List<CodecReader> wrapped = new ArrayList<>(reader.leaves().size());
        for (LeafReaderContext ctx : reader.leaves()) {
          // addIndexes does NOT auto-sort (LUCENE-8505); the SortingCodecReader wrap performs the
          // (merge-based) re-sort of each segment.
          wrapped.add(
              SortingCodecReader.wrap(SlowCodecReaderWrapper.wrap(ctx.reader()), indexSort));
        }
        resortWriter.addIndexes(wrapped.toArray(new CodecReader[0]));
        resortWriter.commit();
      }
    } finally {
      if (currentDir != null) df.release(currentDir);
      if (resortDir != null) df.release(resortDir);
    }

    if (!core.modifyIndexProps(resortIndexName)) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "Failed to point core " + core.getName() + " at the re-sorted index");
    }
    try {
      core.getUpdateHandler().newIndexWriter(false);
      openNewSearcher(core);
      // The core now points at the re-sorted index; drop the previous (unsorted) index directory.
      core.cleanupOldIndexDirectories(false);
    } catch (Exception e) {
      log.warn("Could not switch to re-sorted index for core {}; rolling back", core.getName(), e);
      rollbackIndexProps(core, df, lockType);
      core.getUpdateHandler().newIndexWriter(false);
      openNewSearcher(core);
      // Discard the orphaned re-sort directory we just wrote but never swapped in.
      removeDirectory(df, resortIndexPath, lockType);
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "Failed to swap in the re-sorted index for core " + core.getName(),
          e);
    }
  }

  private void removeDirectory(DirectoryFactory df, String path, String lockType) {
    try {
      if (df.exists(path)) {
        Directory dir = df.get(path, DirectoryFactory.DirContext.DEFAULT, lockType);
        try {
          df.remove(dir, true);
        } finally {
          df.release(dir);
        }
      }
    } catch (Exception cleanupError) {
      log.warn("Could not remove re-sort directory {}", path, cleanupError);
    }
  }

  private void rollbackIndexProps(SolrCore core, DirectoryFactory df, String lockType) {
    Directory dataDir = null;
    try {
      dataDir = df.get(core.getDataDir(), DirectoryFactory.DirContext.META_DATA, lockType);
      dataDir.deleteFile(IndexFetcher.INDEX_PROPERTIES);
    } catch (Exception rollbackError) {
      log.error("Rollback of index.properties failed for core {}", core.getName(), rollbackError);
    } finally {
      if (dataDir != null) {
        try {
          df.release(dataDir);
        } catch (IOException ignored) {
        }
      }
    }
  }

  private void openNewSearcher(SolrCore core) throws Exception {
    final Future<?>[] waitSearcher = new Future<?>[1];
    core.getSearcher(true, false, waitSearcher, true);
    if (waitSearcher[0] != null) {
      waitSearcher[0].get();
    }
  }
}
