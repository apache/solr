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
    apiBag.registerObject(new SchemaInfoAPI(getRequestHandler()));
    apiBag.registerObject(new SchemaUniqueKeyAPI(getRequestHandler()));
    apiBag.registerObject(new SchemaVersionAPI(getRequestHandler()));
    apiBag.registerObject(new SchemaSimilarityAPI(getRequestHandler()));
    apiBag.registerObject(new SchemaZkVersionAPI(getRequestHandler()));
    apiBag.registerObject(new SchemaListAllFieldsAPI(getRequestHandler()));
    apiBag.registerObject(new SchemaGetFieldAPI(getRequestHandler()));
    apiBag.registerObject(new SchemaListAllCopyFieldsAPI(getRequestHandler()));
    apiBag.registerObject(new SchemaListAllDynamicFieldsAPI(getRequestHandler()));
    apiBag.registerObject(new SchemaGetDynamicFieldAPI(getRequestHandler()));
    apiBag.registerObject(new SchemaListAllFieldTypesAPI(getRequestHandler()));
    apiBag.registerObject(new SchemaGetFieldTypeAPI(getRequestHandler()));
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
  public void testGetSchemaInfoApis() {
    assertAnnotatedApiExistsFor("GET", "/schema");
    assertAnnotatedApiExistsFor("GET", "/schema/dynamicfields");
    assertAnnotatedApiExistsFor("GET", "/schema/dynamicfields/someDynamicField");
    assertAnnotatedApiExistsFor("GET", "/schema/fieldtypes");
    assertAnnotatedApiExistsFor("GET", "/schema/fieldtypes/someFieldType");
    assertAnnotatedApiExistsFor("GET", "/schema/fields");
    assertAnnotatedApiExistsFor("GET", "/schema/fields/someField");
    assertAnnotatedApiExistsFor("GET", "/schema/copyfields");
    assertAnnotatedApiExistsFor("GET", "/schema/similarity");
    assertAnnotatedApiExistsFor("GET", "/schema/uniquekey");
    assertAnnotatedApiExistsFor("GET", "/schema/version");
    assertAnnotatedApiExistsFor("GET", "/schema/zkversion");
  }

  @Test
  public void testSchemaBulkModificationApiMapping() {
    assertAnnotatedApiExistsFor("POST", "/schema");
  }
}
