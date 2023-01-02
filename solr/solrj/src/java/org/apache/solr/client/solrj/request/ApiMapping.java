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

package org.apache.solr.client.solrj.request;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.common.util.CommandOperation;

/** Types and utilities used by many ApiMapping classes. */
public class ApiMapping {
  public interface V2EndPoint {

    String getSpecName();
  }

  public interface CommandMeta {
    String getName();

    /** the http method supported by this command */
    SolrRequest.METHOD getHttpMethod();

    V2EndPoint getEndPoint();

    default Iterator<String> getParamNamesIterator(CommandOperation op) {
      return getParamNames_(op, CommandMeta.this).iterator();
    }

    /** Given a v1 param, return the v2 attribute (possibly a dotted path). */
    default String getParamSubstitute(String name) {
      return name;
    }
  }

  private static Collection<String> getParamNames_(CommandOperation op, CommandMeta command) {
    Object o = op.getCommandData();
    if (o instanceof Map) {
      @SuppressWarnings("unchecked")
      Map<String, Object> map = (Map<String, Object>) o;
      List<String> result = new ArrayList<>();
      collectKeyNames(map, result, "");
      return result;
    } else {
      return Collections.emptySet();
    }
  }

  @SuppressWarnings({"unchecked"})
  public static void collectKeyNames(Map<String, Object> map, List<String> result, String prefix) {
    for (Map.Entry<String, Object> e : map.entrySet()) {
      if (e.getValue() instanceof Map) {
        collectKeyNames((Map<String, Object>) e.getValue(), result, prefix + e.getKey() + ".");
      } else {
        result.add(prefix + e.getKey());
      }
    }
  }
}
