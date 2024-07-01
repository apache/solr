/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.solr.monitor;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import org.apache.lucene.monitor.CustomQueryHandler;
import org.apache.lucene.monitor.TermFilteredPresearcher;
import org.apache.lucene.monitor.TermWeightor;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;

public class AliasingTermFilteredPresearcher extends TermFilteredPresearcher {

  private final String prefix;

  AliasingTermFilteredPresearcher(
      TermWeightor weightor,
      List<CustomQueryHandler> customQueryHandlers,
      Set<String> filterFields,
      String prefix) {
    super(weightor, customQueryHandlers, filterFields);
    this.prefix = prefix;
  }

  public static AliasingPresearcher build(
      TermWeightor weightor,
      List<CustomQueryHandler> queryHandlers,
      Set<String> filterFields,
      String prefix) {
    return new AliasingPresearcher(
        new AliasingTermFilteredPresearcher(weightor, queryHandlers, filterFields, prefix), prefix);
  }

  @Override
  protected DocumentQueryBuilder getQueryBuilder() {
    return new AliasingDocumentQueryBuilder(super.getQueryBuilder(), prefix);
  }

  static class AliasingDocumentQueryBuilder implements DocumentQueryBuilder {

    private final DocumentQueryBuilder documentQueryBuilder;
    private final String prefix;

    public AliasingDocumentQueryBuilder(DocumentQueryBuilder documentQueryBuilder, String prefix) {
      this.documentQueryBuilder = documentQueryBuilder;
      this.prefix = prefix;
    }

    @Override
    public void addTerm(String field, BytesRef term) throws IOException {
      documentQueryBuilder.addTerm(prefix + field, term);
    }

    @Override
    public Query build() {
      return documentQueryBuilder.build();
    }
  }
}
