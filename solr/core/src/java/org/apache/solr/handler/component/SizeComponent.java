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
package org.apache.solr.handler.component;

import java.io.IOException;
import java.util.Arrays;
import org.apache.lucene.index.BaseCompositeReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Accountable;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SizeParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.DirectoryFactory;
import org.apache.solr.util.NumberUtils;

/** This component provides sizing estimates based on current state and specified assumptions. */
public class SizeComponent extends SearchComponent {
  public static final String COMPONENT_NAME = "size";
  public static final long DEFAULT_DOC_SIZE = 10 * 1024; // Default to 10K docs

  @Override
  public void prepare(ResponseBuilder rb) throws IOException {
    SolrParams params = rb.req.getParams();
    String sizeUnit = params.get(SizeParams.SIZE_UNIT);
    if (sizeUnit != null) {
      try {
        NumberUtils.sizeUnit.valueOf(NumberUtils.sizeUnit.class, sizeUnit);
      } catch (IllegalArgumentException e) {
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST,
            "Unsupported size unit:"
                + sizeUnit
                + ".  Valid values are "
                + Arrays.asList(NumberUtils.sizeUnit.values()),
            e);
      }
    }
  }

  @Override
  public void process(ResponseBuilder rb) throws IOException {
    if (rb.req.getParams().getBool(SizeParams.SIZE, false)) {
      SolrParams params = rb.req.getParams();
      DirectoryReader indexReader = rb.req.getSearcher().getIndexReader();

      Counters counters = new Counters();
      Settings settings = new Settings(indexReader, params);

      Directory dir = indexReader.directory();
      long size = DirectoryFactory.sizeOfDirectory(dir);
      counters.totalDiskSize = (long) (size * settings.estimationRatio);
      counters.totalLuceneRam =
          (long)
              (counters.totalLuceneRam + getIndexHeapUsed(indexReader) * settings.estimationRatio);

      String sizeUnit = params.get(SizeParams.SIZE_UNIT);
      counters.computeRam(settings);
      rb.rsp.add("size", counters.toNamedList(sizeUnit));
    }
  }

  /** Returns the sum of RAM bytes used by each segment */
  private static long getIndexHeapUsed(DirectoryReader reader) {
    return reader.leaves().stream()
        .map(LeafReaderContext::reader)
        .map(FilterLeafReader::unwrap)
        .map(
            leafReader -> {
              if (leafReader instanceof Accountable) {
                return ((Accountable) leafReader).ramBytesUsed();
              } else {
                return -1L; // unsupported
              }
            })
        .mapToLong(Long::longValue)
        .reduce(0, (left, right) -> left == -1 || right == -1 ? -1 : left + right);
    // if any leaves are unsupported (-1), we ultimately return -1.
  }

  @Override
  public String getDescription() {
    return "Size estimator";
  }

  private static class Settings {
    final long averageDocSize;
    final double actualNumDocs;
    double numDocsForEstimation;
    double estimationRatio;
    final double actualDeletedDocs;
    final double deletedDocsForEstimation;
    final double filterCacheMax;
    final double queryResultCacheMax;
    final double documentCacheMax;
    final double queryResultMaxDocsCached;

    public Settings(BaseCompositeReader<LeafReader> indexReader, SolrParams params) {
      averageDocSize = params.getLong(SizeParams.AVG_DOC_SIZE, 0L);
      actualNumDocs = indexReader.numDocs();
      numDocsForEstimation = params.getLong(SizeParams.NUM_DOCS, 0L);
      estimationRatio = params.getDouble(SizeParams.ESTIMATION_RATIO, 0.0);
      if (estimationRatio > 0 && numDocsForEstimation <= 0) {
        numDocsForEstimation = estimationRatio * actualNumDocs;
      } else if (numDocsForEstimation > 0) {
        estimationRatio = numDocsForEstimation / actualNumDocs;
      } else {
        estimationRatio = 1.0;
        numDocsForEstimation = actualNumDocs;
      }
      actualDeletedDocs = indexReader.numDeletedDocs();
      deletedDocsForEstimation = params.getLong(SizeParams.DELETED_DOCS, (long) actualDeletedDocs);
      filterCacheMax = params.getLong(SizeParams.FILTER_CACHE_MAX, 512);
      queryResultCacheMax = params.getLong(SizeParams.QUERY_RES_CACHE_MAX, 512);
      documentCacheMax = params.getLong(SizeParams.DOC_CACHE_MAX, 512);
      queryResultMaxDocsCached = params.getLong(SizeParams.QUERY_RES_MAX_DOCS, 200);
    }
  }

  private static class Counters {
    long totalDiskSize = 0;
    long totalLuceneRam = 0;
    long totalSolrRam = 0;
    private long filterCacheRam;
    private long queryResultCacheRam;
    private long documentCacheRam;
    private long avgDocSize;
    private long totalNumDocs;

    public void computeRam(Settings settings) {
      totalLuceneRam = (long) (totalLuceneRam + settings.deletedDocsForEstimation / 8);
      totalLuceneRam += 32 * 1024 * 1024; // 32MB RAM buffer size
      filterCacheRam =
          (long)
              (settings.numDocsForEstimation * settings.filterCacheMax / 8
                  + settings.filterCacheMax * 20);
      totalSolrRam += filterCacheRam;
      queryResultCacheRam =
          (long)
              (Math.min(settings.queryResultMaxDocsCached, settings.numDocsForEstimation)
                      * 8
                      * settings.queryResultCacheMax
                  + settings.queryResultCacheMax * 400);
      totalSolrRam += queryResultCacheRam;
      if (settings.averageDocSize > 0) {
        avgDocSize = settings.averageDocSize;
      } else if (totalDiskSize > 0 && settings.numDocsForEstimation > 0) {
        avgDocSize = (long) (totalDiskSize / settings.numDocsForEstimation);
      } else {
        avgDocSize = DEFAULT_DOC_SIZE;
      }
      documentCacheRam =
          (long) (avgDocSize * Math.min(settings.numDocsForEstimation, settings.documentCacheMax));
      totalSolrRam += documentCacheRam;
      totalSolrRam += totalLuceneRam;
      totalNumDocs = (long) settings.numDocsForEstimation;
    }

    public NamedList<Object> toNamedList(final String sizeUnit) {
      NamedList<Object> values = new NamedList<>();
      NamedList<Object> solrRamDetails = new NamedList<>();

      values.add(
          "totalDiskSize",
          sizeUnit == null
              ? NumberUtils.readableSize(totalDiskSize)
              : NumberUtils.normalizedSize(totalDiskSize, sizeUnit));
      values.add(
          "totalLuceneRam",
          sizeUnit == null
              ? NumberUtils.readableSize(totalLuceneRam)
              : NumberUtils.normalizedSize(totalLuceneRam, sizeUnit));
      values.add(
          "totalSolrRam",
          sizeUnit == null
              ? NumberUtils.readableSize(totalSolrRam)
              : NumberUtils.normalizedSize(totalSolrRam, sizeUnit));
      values.add("estimatedNumDocs", totalNumDocs);
      values.add(
          "estimatedDocSize",
          sizeUnit == null
              ? NumberUtils.readableSize(avgDocSize)
              : NumberUtils.normalizedSize(avgDocSize, sizeUnit));

      solrRamDetails.add(
          "filterCache",
          sizeUnit == null
              ? NumberUtils.readableSize(filterCacheRam)
              : NumberUtils.normalizedSize(filterCacheRam, sizeUnit));
      solrRamDetails.add(
          "queryResultCache",
          sizeUnit == null
              ? NumberUtils.readableSize(queryResultCacheRam)
              : NumberUtils.normalizedSize(queryResultCacheRam, sizeUnit));
      solrRamDetails.add(
          "documentCache",
          sizeUnit == null
              ? NumberUtils.readableSize(documentCacheRam)
              : NumberUtils.normalizedSize(documentCacheRam, sizeUnit));
      solrRamDetails.add(
          "luceneRam",
          sizeUnit == null
              ? NumberUtils.readableSize(totalLuceneRam)
              : NumberUtils.normalizedSize(totalLuceneRam, sizeUnit));
      values.add("solrDetails", solrRamDetails);
      return values;
    }
  }
}
