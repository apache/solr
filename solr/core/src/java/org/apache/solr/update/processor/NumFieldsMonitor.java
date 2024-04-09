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
package org.apache.solr.update.processor;

import org.apache.solr.core.AbstractSolrEventListener;
import org.apache.solr.core.SolrCore;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.RefCounted;

public class NumFieldsMonitor extends AbstractSolrEventListener {

  private int numFields;

  public NumFieldsMonitor(SolrCore core) {
    super(core);
    this.numFields = -1;
  }

  @Override
  public void postCommit() {
    RefCounted<SolrIndexSearcher> indexSearcher = null;
    try {
      indexSearcher = getCore().getSearcher();
      // Get the number of fields directly from the IndexReader instead of the Schema object to also
      // include the
      // fields that are missing from the Schema file (dynamic/deleted etc.)
      numFields = indexSearcher.get().getFieldInfos().size();
    } finally {
      if (indexSearcher != null) {
        indexSearcher.decref();
      }
    }
  }

  public int getCurrentNumFields() {
    return numFields;
  }
}
