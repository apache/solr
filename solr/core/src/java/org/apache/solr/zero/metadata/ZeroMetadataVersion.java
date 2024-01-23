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
package org.apache.solr.zero.metadata;

import org.apache.solr.common.cloud.DocCollection;

/**
 * This represents the metadata suffix and the corresponding Zookeeper version of the node for a
 * shard of a Zero collection {@link DocCollection#isZeroIndex()}
 */
public class ZeroMetadataVersion {
  /** The metadataSuffix for the shard stored in the Zero store. */
  private final String metadataSuffix;

  /** version of zookeeper node maintaining the metadata */
  private final int version;

  public ZeroMetadataVersion(String metadataSuffix, int version) {
    assert metadataSuffix != null;
    this.metadataSuffix = metadataSuffix;
    this.version = version;
  }

  public String getMetadataSuffix() {
    return metadataSuffix;
  }

  public int getVersion() {
    return version;
  }

  @Override
  public String toString() {
    return "metadataSuffix=" + metadataSuffix + " version=" + version;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null || obj.getClass() != this.getClass()) {
      return false;
    }

    final ZeroMetadataVersion other = (ZeroMetadataVersion) obj;
    return this.version == other.version && this.metadataSuffix.equals(other.metadataSuffix);
  }

  @Override
  public int hashCode() {
    // This object is never inserted into a collection
    return 0;
  }
}
