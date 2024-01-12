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
package org.apache.solr.client.api.endpoint;

import io.swagger.v3.oas.annotations.Operation;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import org.apache.solr.client.api.model.BalanceShardUniqueRequestBody;
import org.apache.solr.client.api.model.SubResponseAccumulatingJerseyResponse;

/**
 * V2 API definition for insuring that a specified per-shard property is distributed evenly amongst
 * the physical nodes comprising a collection.
 *
 * <p>The new API (POST /v2/collections/collectionName/balance-shard-unique {...} ) is analogous to
 * the v1 /admin/collections?action=BALANCESHARDUNIQUE command.
 */
@Path("/collections/{collectionName}/balance-shard-unique")
public interface BalanceShardUniqueApi {
  @POST
  @Operation(
      summary =
          "Ensure a specified per-shard property is distributed evenly amongst physical nodes comprising a collection",
      tags = {"collections"})
  SubResponseAccumulatingJerseyResponse balanceShardUnique(
      @PathParam("collectionName") String collectionName, BalanceShardUniqueRequestBody requestBody)
      throws Exception;
}
