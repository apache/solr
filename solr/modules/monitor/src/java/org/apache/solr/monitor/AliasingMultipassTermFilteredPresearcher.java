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

import static org.apache.solr.monitor.AliasingTermFilteredPresearcher.AliasingDocumentQueryBuilder;

import java.util.List;
import java.util.Set;
import org.apache.lucene.monitor.CustomQueryHandler;
import org.apache.lucene.monitor.MultipassTermFilteredPresearcher;
import org.apache.lucene.monitor.TermWeightor;

public class AliasingMultipassTermFilteredPresearcher extends MultipassTermFilteredPresearcher {

  private final String prefix;

  private AliasingMultipassTermFilteredPresearcher(
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
    return new AliasingDocumentQueryBuilder(super.getQueryBuilder(), prefix);
  }

  public static AliasingPresearcher build(
      int passes,
      float minWeight,
      TermWeightor weightor,
      List<CustomQueryHandler> queryHandlers,
      Set<String> filterFields,
      String prefix) {
    return new AliasingPresearcher(
        new AliasingMultipassTermFilteredPresearcher(
            passes, minWeight, weightor, queryHandlers, filterFields, prefix),
        prefix);
  }
}
