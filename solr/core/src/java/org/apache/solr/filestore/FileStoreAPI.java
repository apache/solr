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

package org.apache.solr.filestore;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.util.Utils;

public class FileStoreAPI {
  public static final String KEYS_DIR = "/_trusted_/keys";

  public static class MetaData implements MapWriter {
    public static final String SHA512 = "sha512";
    String sha512;
    List<String> signatures;
    Map<String, Object> otherAttribs;

    @SuppressWarnings("unchecked")
    public MetaData(Map<String, Object> m) {
      m = (Map<String, Object>) Utils.getDeepCopy(m, 3);
      this.sha512 = (String) m.remove(SHA512);
      this.signatures = (List<String>) m.remove("sig");
      this.otherAttribs = m;
    }

    @Override
    public void writeMap(EntryWriter ew) throws IOException {
      ew.putIfNotNull("sha512", sha512);
      ew.putIfNotNull("sig", signatures);
      if (!otherAttribs.isEmpty()) {
        otherAttribs.forEach(ew.getBiConsumer());
      }
    }

    @Override
    public int hashCode() {
      return sha512.hashCode();
    }

    @Override
    public boolean equals(Object that) {
      if (that instanceof MetaData) {
        MetaData metaData = (MetaData) that;
        return Objects.equals(sha512, metaData.sha512)
            && Objects.equals(signatures, metaData.signatures)
            && Objects.equals(otherAttribs, metaData.otherAttribs);
      }
      return false;
    }
  }
}
