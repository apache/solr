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
package org.apache.solr.request.macro;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class MacroSanitizer {

  /**
   * Sanitizes macros for the given parameter in the given params set if present
   *
   * @param param the parameter whose values we should sanitzize
   * @param params the parameter set
   * @return the sanitized parameter set
   */
  public static Map<String, String[]> sanitize(String param, Map<String, String[]> params) {
    // quick peek into the values to check for macros
    final boolean needsSanitizing =
        params.containsKey(param)
            && Arrays.stream(params.get(param))
                .anyMatch(s -> s.contains(MacroExpander.MACRO_START));

    if (needsSanitizing) {
      final String[] fqs = params.get(param);
      final List<String> sanitizedFqs = new ArrayList<>(fqs.length);

      for (int i = 0; i < fqs.length; i++) {
        if (!fqs[i].contains(MacroExpander.MACRO_START)) {
          sanitizedFqs.add(fqs[i]);
        }
      }

      params.put(param, sanitizedFqs.toArray(new String[] {}));
    }

    return params;
  }
}
