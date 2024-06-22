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
package org.apache.solr.client.solrj.io.stream;

import java.util.List;
import java.util.Map;

class StreamAssert {
  static boolean assertMaps(List<Map<?, ?>> maps, int... ids) throws Exception {
    if (maps.size() != ids.length) {
      throw new Exception(
          "Expected id count != actual map count:" + ids.length + ":" + maps.size());
    }

    int i = 0;
    for (int val : ids) {
      Map<?, ?> t = maps.get(i);
      String tip = (String) t.get("id");
      if (!tip.equals(Integer.toString(val))) {
        throw new Exception("Found value:" + tip + " expecting:" + val);
      }
      ++i;
    }
    return true;
  }

  static boolean assertList(List<?> list, Object... vals) throws Exception {
    if (list.size() != vals.length) {
      throw new Exception("Lists are not the same size:" + list.size() + " : " + vals.length);
    }

    for (int i = 0; i < list.size(); i++) {
      Object a = list.get(i);
      Object b = vals[i];
      if (!a.equals(b)) {
        throw new Exception("List items not equals:" + a + " : " + b);
      }
    }

    return true;
  }
}
