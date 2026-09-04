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
package org.apache.solr.crossdc.common;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public abstract class CrossDcConf {
  public static final String CROSSDC_PROPERTIES = "/crossdc.properties";
  public static final String ZK_CROSSDC_PROPS_PATH = "zkCrossDcPropsPath";
  public static final String EXPAND_DBQ = "solr.crossdc.expandDbq";
  public static final String COLLAPSE_UPDATES = "solr.crossdc.collapseUpdates";
  public static final String MAX_COLLAPSE_RECORDS = "solr.crossdc.maxCollapseRecords";

  /** Option to expand Delete-By-Query requests on the producer side. */
  public enum ExpandDbq {
    /** Don't expand DBQs, mirror them as-is. */
    NONE,
    /**
     * Expand DBQs into multiple Delete-By-Id requests using matching documents on the producer
     * side.
     */
    EXPAND;

    private static final Map<String, ExpandDbq> valueMap = new HashMap<>();

    static {
      for (ExpandDbq value : values()) {
        valueMap.put(value.name().toUpperCase(Locale.ROOT), value);
      }
    }

    public static ExpandDbq getOrDefault(String strValue, ExpandDbq defaultValue) {
      if (strValue == null || strValue.isBlank()) {
        return defaultValue;
      }
      ExpandDbq value = valueMap.get(strValue.toUpperCase(Locale.ROOT));
      return value != null ? value : defaultValue;
    }
  }

  /**
   * Option to collapse multiple update requests in the Consumer application before sending to the
   * target Solr.
   */
  public enum CollapseUpdates {
    /** Don't collapse any update requests, send them directly as-is. */
    NONE,
    /** Collapse only update requests that don't contain any delete operations. */
    PARTIAL,
    /** Always collapse update requests, as long as they have the same parameters. */
    ALL;

    private static final Map<String, CollapseUpdates> valueMap = new HashMap<>();

    static {
      for (CollapseUpdates value : values()) {
        valueMap.put(value.name().toUpperCase(Locale.ROOT), value);
      }
    }

    public static CollapseUpdates getOrDefault(String strValue, CollapseUpdates defaultValue) {
      if (strValue == null || strValue.isBlank()) {
        return defaultValue;
      }
      CollapseUpdates value = valueMap.get(strValue.toUpperCase(Locale.ROOT));
      return value != null ? value : defaultValue;
    }
  }
}
