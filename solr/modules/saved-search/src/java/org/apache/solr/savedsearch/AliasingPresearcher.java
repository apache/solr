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

package org.apache.solr.savedsearch;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiPredicate;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.monitor.CustomQueryHandler;
import org.apache.lucene.monitor.MultipassTermFilteredPresearcher;
import org.apache.lucene.monitor.Presearcher;
import org.apache.lucene.monitor.TermFilteredPresearcher;
import org.apache.lucene.monitor.TermWeightor;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;

/**
 * The motivation behind this is to map each document field to its corresponding, presearcher-owned
 * equivalent. By prefixing we ensure "presearcher fields" don't inadvertently affect the scores of
 * "real" document fields. Another issue is that the presearcher can dynamically create field
 * configurations for a given field name that might not be compatible with the pre-existing schema
 * definition, see {\@link
 * org.apache.solr.savedsearch.SingleCoreSavedSearchTest#coexistWithRegularDocumentsTest}. In the
 * case of {@link org.apache.lucene.monitor.MultipassTermFilteredPresearcher#field(String, int)},
 * the presearcher is capable of creating new fields by adding an ordinal suffix to an existing
 * field name. This could also clash with user-defined name patterns, hence the existence of this
 * class, which allows users to alias presearcher fields as they see fit.
 */
public class AliasingPresearcher extends Presearcher {

  private final Presearcher aliasingPresearcher;
  private final String prefix;

  AliasingPresearcher(Presearcher aliasingPresearcher, String prefix) {
    this.aliasingPresearcher = aliasingPresearcher;
    this.prefix = prefix;
  }

  @Override
  public Query buildQuery(LeafReader reader, BiPredicate<String, BytesRef> termAcceptor) {
    BiPredicate<String, BytesRef> aliasingTermAcceptor =
        (fieldName, term) -> termAcceptor.test(prefix + fieldName, term);
    return aliasingPresearcher.buildQuery(reader, aliasingTermAcceptor);
  }

  @Override
  public Document indexQuery(Query query, Map<String, String> metadata) {
    var document = aliasingPresearcher.indexQuery(query, metadata);
    return alias(document);
  }

  public String getPrefix() {
    return prefix;
  }

  private Document alias(Document in) {
    Document out = new Document();
    for (var field : in) {
      if (!TermFilteredPresearcher.ANYTOKEN_FIELD.equals(field.name())
          && field instanceof Field
          && ((Field) field).tokenStreamValue() != null) {
        out.add(
            new Field(
                prefix + field.name(), ((Field) field).tokenStreamValue(), field.fieldType()));
      } else {
        out.add(field);
      }
    }
    return out;
  }

  public static class MultiPassTermFiltered extends MultipassTermFilteredPresearcher {

    private final String prefix;

    private MultiPassTermFiltered(
        int passes,
        float minWeight,
        TermWeightor weightor,
        List<CustomQueryHandler> queryHandlers,
        Set<String> filterFields,
        String prefix) {
      super(passes, minWeight, weightor, queryHandlers, filterFields);
      this.prefix = prefix;
    }

    @Override
    protected DocumentQueryBuilder getQueryBuilder() {
      return new TermFiltered.AliasingDocumentQueryBuilder(super.getQueryBuilder(), prefix);
    }

    public static AliasingPresearcher build(
        int passes,
        float minWeight,
        TermWeightor weightor,
        List<CustomQueryHandler> queryHandlers,
        Set<String> filterFields,
        String prefix) {
      return new AliasingPresearcher(
          new MultiPassTermFiltered(
              passes, minWeight, weightor, queryHandlers, filterFields, prefix),
          prefix);
    }
  }

  public static class TermFiltered extends TermFilteredPresearcher {

    private final String prefix;

    TermFiltered(
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
          new TermFiltered(weightor, queryHandlers, filterFields, prefix), prefix);
    }

    @Override
    protected DocumentQueryBuilder getQueryBuilder() {
      return new AliasingDocumentQueryBuilder(super.getQueryBuilder(), prefix);
    }

    static class AliasingDocumentQueryBuilder implements DocumentQueryBuilder {

      private final DocumentQueryBuilder documentQueryBuilder;
      private final String prefix;

      public AliasingDocumentQueryBuilder(
          DocumentQueryBuilder documentQueryBuilder, String prefix) {
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
}
