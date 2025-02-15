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

package org.apache.solr.handler.admin.api;

import org.apache.solr.handler.SchemaHandler;
import org.apache.solr.handler.admin.V2ApiMappingTest;
import org.junit.Test;

/** Unit tests for the GET and POST /v2/c/collectionName/schema APIs. */
public class V2SchemaAPIMappingTest extends V2ApiMappingTest<SchemaHandler> {

  @Override
  public void populateApiBag() {
    apiBag.registerObject(new SchemaBulkModifyAPI(getRequestHandler()));
  }

  @Override
  public SchemaHandler createUnderlyingRequestHandler() {
    return createMock(SchemaHandler.class);
  }

  @Override
  public boolean isCoreSpecific() {
    return true;
  }

  @Test
  public void testSchemaBulkModificationApiMapping() {
    assertAnnotatedApiExistsFor("POST", "/schema");
  }
}
