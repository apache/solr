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

/** Defines the request parameters used by all combiners. */
public interface CombinerParams {
  String COMBINER = "combiner";

  String COMBINER_ALGORITHM = COMBINER + ".algorithm";

  String COMBINER_KEYS = COMBINER + ".keys";

  String RECIPROCAl_RANK_FUSION = "rrf";

  String COMBINER_UP_TO = COMBINER + ".upTo";

  int COMBINER_UP_TO_DEFAULT = 100;

  String COMBINER_RRF_K = COMBINER + "." + RECIPROCAl_RANK_FUSION + ".k";

  int COMBINER_RRF_K_DEFAULT = 60;
}
