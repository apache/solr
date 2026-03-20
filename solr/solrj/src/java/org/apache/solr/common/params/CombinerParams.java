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
package org.apache.solr.common.params;

/**
 * This class provides constants for configuration parameters related to the combiner. It defines
 * keys for various properties used in the combiner configuration.
 */
public class CombinerParams {

  private CombinerParams() {}

  public static final String COMBINER = "combiner";
  public static final String COMBINER_ALGORITHM = COMBINER + ".algorithm";
  public static final String COMBINER_QUERY = COMBINER + ".query";
  public static final String RECIPROCAL_RANK_FUSION = "rrf";
  public static final String COMBINER_RRF_K = COMBINER + "." + RECIPROCAL_RANK_FUSION + ".k";
  public static final String DEFAULT_COMBINER = RECIPROCAL_RANK_FUSION;
  public static final int DEFAULT_COMBINER_RRF_K = 60;
  public static final int DEFAULT_MAX_COMBINER_QUERIES = 5;
}
