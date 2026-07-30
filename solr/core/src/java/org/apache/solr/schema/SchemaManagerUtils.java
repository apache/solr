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
package org.apache.solr.schema;

import static org.apache.solr.client.api.model.SchemaChange.OPERATION_TYPE_PROP;

import com.fasterxml.jackson.core.type.TypeReference;
import java.util.Map;
import org.apache.solr.client.api.model.SchemaChange;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.CommandOperation;
import org.apache.solr.jersey.SolrJacksonMapper;

public class SchemaManagerUtils {
  public static SchemaChange convertToSchemaChangeOperations(CommandOperation toConvert) {
    final var opName = toConvert.name;
    if (SchemaManager.OpType.get(opName) == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "No such operation: " + opName);
    }

    final var operationData = toConvert.getDataMap();
    operationData.put(OPERATION_TYPE_PROP, opName);

    return SolrJacksonMapper.getObjectMapper().convertValue(operationData, SchemaChange.class);
  }

  // TODO This utility exists because ManagedIndexSchema currently consumes op data in "Map" form.
  // This should be switched over to using SchemaChangeOperation, and this utility removed.
  public static Map<String, Object> convertToMap(SchemaChange toConvert) {

    return convertToMapExcluding(toConvert, OPERATION_TYPE_PROP);
  }

  public static Map<String, Object> convertToMapExcluding(
      SchemaChange toConvert, String... propsToOmit) {
    final var opMap =
        SolrJacksonMapper.getObjectMapper()
            .convertValue(toConvert, new TypeReference<Map<String, Object>>() {});
    if (propsToOmit != null) {
      for (String prop : propsToOmit) {
        opMap.remove(prop);
      }
    }
    return opMap;
  }
}
