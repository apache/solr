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

package org.apache.solr.handler.admin;

import org.apache.solr.api.AnnotatedApi;
import org.apache.solr.handler.BlobHandler;
import org.apache.solr.handler.admin.api.GetBlobInfoAPI;
import org.apache.solr.handler.admin.api.UploadBlobAPI;
import org.junit.Test;

public class V2BlobAPIMappingTest extends V2ApiMappingTest<BlobHandler> {
  @Override
  public void populateApiBag() {
    apiBag.registerObject(new GetBlobInfoAPI(getRequestHandler()));
    apiBag.registerObject(new UploadBlobAPI(getRequestHandler()));
  }

  @Override
  public BlobHandler createUnderlyingRequestHandler() {
    return createMock(BlobHandler.class);
  }

  @Override
  public boolean isCoreSpecific() {
    return true;
  }

  @Test
  public void testGetBlobApiMappings() {
    assertAnnotatedApiExistsFor("GET", "/blob");
    assertAnnotatedApiExistsFor("GET", "/blob/someBlobName");
    assertAnnotatedApiExistsFor("GET", "/blob/someBlobName/123");
  }

  @Test
  public void testUploadBlobApiMapping() {
    final AnnotatedApi uploadBlobApi = assertAnnotatedApiExistsFor("POST", "/blob/someBlobName");
    assertEquals(1, uploadBlobApi.getCommands().keySet().size());
    // Empty-string is the indicator for POST requests that don't use the explicit "command" syntax.
    assertEquals("", uploadBlobApi.getCommands().keySet().iterator().next());
  }
}
