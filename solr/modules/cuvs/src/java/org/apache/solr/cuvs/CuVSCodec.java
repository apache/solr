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
  private static final String FALLBACK_CODEC = "Lucene103";
  private final SolrCore core;
  private final Lucene103Codec fallbackCodec;

  public CuVSCodec(SolrCore core, Lucene103Codec fallback, NamedList<?> args) {
    super(FALLBACK_CODEC, fallback);
    this.core = core;
    this.fallbackCodec = fallback;
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
            if (DenseVectorField.CAGRA_HNSW_ALGORITHM.equals(knnAlgorithm)) {

              int cuvsWriterThreads = vectorType.getCuvsWriterThreads();
              int cuvsIntGraphDegree = vectorType.getCuvsIntGraphDegree();
              int cuvsGraphDegree = vectorType.getCuvsGraphDegree();
              int cuvsHnswLayers = vectorType.getCuvsHnswLayers();
              int cuvsHnswM = vectorType.getCuvsHnswMaxConn();
              int cuvsHNSWEfConstruction = vectorType.getCuvsHnswEfConstruction();

              assert cuvsWriterThreads > 0 : "cuvsWriterThreads cannot be less then or equal to 0";
              assert cuvsIntGraphDegree > 0
                  : "cuvsIntGraphDegree cannot be less then or equal to 0";
              assert cuvsGraphDegree > 0 : "cuvsGraphDegree cannot be less then or equal to 0";
              assert cuvsHnswLayers > 0 : "cuvsHnswLayers cannot be less then or equal to 0";
              assert cuvsHnswM > 0 : "cuvsHnswM cannot be less then or equal to 0";
              assert cuvsHNSWEfConstruction > 0
                  : "cuvsHNSWEfConstruction cannot be less then or equal to 0";

              if (log.isInfoEnabled()) {
                log.info(
                    "Initializing Lucene99AcceleratedHNSWVectorsFormat with parameter values: cuvsWriterThreads {}, cuvsIntGraphDegree {}, cuvsGraphDegree {}, cuvsHnswLayers {}, cuvsHnswM {}, cuvsHNSWEfConstruction {}",
                    cuvsWriterThreads,
                    cuvsIntGraphDegree,
                    cuvsGraphDegree,
                    cuvsHnswLayers,
                    cuvsHnswM,
                    cuvsHNSWEfConstruction);
              }
              return new Lucene99AcceleratedHNSWVectorsFormat(
                  cuvsWriterThreads,
                  cuvsIntGraphDegree,
                  cuvsGraphDegree,
                  cuvsHnswLayers,
                  cuvsHnswM,
                  cuvsHNSWEfConstruction);
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
