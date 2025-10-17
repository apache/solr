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
package org.apache.solr.cuvs;

import com.nvidia.cuvs.lucene.Lucene99AcceleratedHNSWVectorsFormat;
import java.lang.invoke.MethodHandles;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.lucene103.Lucene103Codec;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.schema.DenseVectorField;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This codec utilizes the Lucene99AcceleratedHNSWVectorsFormat from the lucene-cuvs library to
 * enable GPU-based accelerated vector search.
 *
 * @since 10.0.0
 */
public class CuVSCodec extends FilterCodec {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final int DEFAULT_CUVS_WRITER_THREADS = 32;
  private static final int DEFAULT_INT_GRAPH_DEGREE = 128;
  private static final int DEFAULT_GRAPH_DEGREE = 64;
  private static final int DEFAULT_HNSW_LAYERS = 1;
  private static final int DEFAULT_M = 16;
  private static final int DEFAULT_EF_CONSTRUCTION = 100;

  private static final String CAGRA_HNSW = "cagra_hnsw";
  private static final String FALLBACK_CODEC = "Lucene103";

  private final SolrCore core;
  private final Lucene103Codec fallbackCodec;
  private final Lucene99AcceleratedHNSWVectorsFormat cuvsHNSWVectorsFormat;

  public CuVSCodec(SolrCore core, Lucene103Codec fallback, NamedList<?> args) {
    super(FALLBACK_CODEC, fallback);
    this.core = core;
    this.fallbackCodec = fallback;

    String cwt = args._getStr("cuvsWriterThreads");
    int cuvsWriterThreads = cwt != null ? Integer.parseInt(cwt) : DEFAULT_CUVS_WRITER_THREADS;
    String igd = args._getStr("intGraphDegree");
    int intGraphDegree = igd != null ? Integer.parseInt(igd) : DEFAULT_INT_GRAPH_DEGREE;
    String gd = args._getStr("graphDegree");
    int graphDegree = gd != null ? Integer.parseInt(gd) : DEFAULT_GRAPH_DEGREE;
    String hl = args._getStr("hnswLayers");
    int hnswLayers = hl != null ? Integer.parseInt(hl) : DEFAULT_HNSW_LAYERS;
    String mc = args._getStr("m");
    int m = mc != null ? Integer.parseInt(mc) : DEFAULT_M;
    String bw = args._getStr("efConstruction");
    int efConstruction = bw != null ? Integer.parseInt(bw) : DEFAULT_EF_CONSTRUCTION;

    assert cuvsWriterThreads > 0 : "cuvsWriterThreads cannot be less then or equal to 0";
    assert intGraphDegree > 0 : "intGraphDegree cannot be less then or equal to 0";
    assert graphDegree > 0 : "graphDegree cannot be less then or equal to 0";
    assert hnswLayers > 0 : "hnswLayers cannot be less then or equal to 0";
    assert m > 0 : "m cannot be less then or equal to 0";
    assert efConstruction > 0 : "efConstruction cannot be less then or equal to 0";

    cuvsHNSWVectorsFormat =
        new Lucene99AcceleratedHNSWVectorsFormat(
            cuvsWriterThreads, intGraphDegree, graphDegree, hnswLayers, m, efConstruction);

    if (log.isInfoEnabled()) {
      log.info(
          "Lucene99AcceleratedHNSWVectorsFormat initialized with parameter values: cuvsWriterThreads {}, intGraphDegree {}, graphDegree {}, hnswLayers {}, m {}, efConstruction {}",
          cuvsWriterThreads,
          intGraphDegree,
          graphDegree,
          hnswLayers,
          m,
          efConstruction);
    }
  }

  @Override
  public KnnVectorsFormat knnVectorsFormat() {
    return perFieldKnnVectorsFormat;
  }

  private PerFieldKnnVectorsFormat perFieldKnnVectorsFormat =
      new PerFieldKnnVectorsFormat() {
        @Override
        public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
          final SchemaField schemaField = core.getLatestSchema().getFieldOrNull(field);
          FieldType fieldType = (schemaField == null ? null : schemaField.getType());
          if (fieldType instanceof DenseVectorField vectorType) {
            String knnAlgorithm = vectorType.getKnnAlgorithm();
            if (CAGRA_HNSW.equals(knnAlgorithm)) {
              return cuvsHNSWVectorsFormat;
            } else if (DenseVectorField.HNSW_ALGORITHM.equals(knnAlgorithm)) {
              return fallbackCodec.getKnnVectorsFormatForField(field);
            } else {
              throw new SolrException(
                  SolrException.ErrorCode.SERVER_ERROR,
                  knnAlgorithm + " KNN algorithm is not supported");
            }
          }
          return fallbackCodec.getKnnVectorsFormatForField(field);
        }
      };
}
