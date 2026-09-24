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

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.solr.client.solrj.RemoteSolrException;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/** Tests the v2 copy-field endpoints, which address rules by their source field. */
public class V2CopyFieldApiTest extends SolrCloudTestCase {

  private static final String COLLECTION = "v2CopyFieldApiTest";
  private static final String SOURCE = "cf_source";
  private static final String DEST_ONE = "cf_dest_one";
  private static final String DEST_TWO = "cf_dest_two";

  @BeforeClass
  public static void setupCluster() throws Exception {
    System.setProperty("managed.schema.mutable", "true");
    configureCluster(1).addConfig("conf", configset("cloud-managed")).configure();

    CollectionAdminRequest.createCollection(COLLECTION, "conf", 1, 1)
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(COLLECTION, 1, 1);

    for (String field : List.of(SOURCE, DEST_ONE, DEST_TWO)) {
      new V2Request.Builder(schemaPath("/fields/" + field))
          .PUT()
          .withPayload(Map.of("type", "string", "multiValued", true))
          .build()
          .process(cluster.getSolrClient());
    }
  }

  @Before
  public void clearCopyFields() throws Exception {
    // Leave each test a clean slate; the source may legitimately have no rules yet.
    try {
      deleteCopyFields(SOURCE);
    } catch (RemoteSolrException e) {
      if (e.code() != 404) {
        throw e;
      }
    }
  }

  private static String schemaPath(String suffix) {
    return "/collections/" + COLLECTION + "/schema" + suffix;
  }

  private static void addCopyFields(String source, Object payload) throws Exception {
    new V2Request.Builder(schemaPath("/copyfields/" + source))
        .PUT()
        .withPayload(payload)
        .build()
        .process(cluster.getSolrClient());
  }

  private static void deleteCopyFields(String source, String... destinations) throws Exception {
    final var params = new ModifiableSolrParams();
    for (String destination : destinations) {
      params.add("destination", destination);
    }
    new V2Request.Builder(schemaPath("/copyfields/" + source))
        .DELETE()
        .withParams(params)
        .build()
        .process(cluster.getSolrClient());
  }

  /** Destinations of every copy-field rule currently declared with the given source. */
  @SuppressWarnings("unchecked")
  private static List<String> destinationsOf(String source) throws Exception {
    final var response =
        new V2Request.Builder(schemaPath("/copyfields"))
            .GET()
            .withParams(new MapSolrParams(Map.of()))
            .build()
            .process(cluster.getSolrClient());
    final var copyFields = (List<Map<String, Object>>) response.getResponse().get("copyFields");
    return copyFields.stream()
        .filter(rule -> source.equals(rule.get("source")))
        .map(rule -> (String) rule.get("dest"))
        .sorted()
        .collect(Collectors.toList());
  }

  @Test
  public void testAddAndListCopyFieldsBySource() throws Exception {
    addCopyFields(SOURCE, Map.of("destinations", List.of(DEST_ONE, DEST_TWO)));

    assertEquals(List.of(DEST_ONE, DEST_TWO), destinationsOf(SOURCE));
  }

  @Test
  public void testAddAcceptsSingleDestinationAndMaxChars() throws Exception {
    addCopyFields(SOURCE, Map.of("destinations", DEST_ONE, "maxChars", 100));

    assertEquals(List.of(DEST_ONE), destinationsOf(SOURCE));
  }

  @Test
  public void testDeleteNarrowedByDestinationParam() throws Exception {
    addCopyFields(SOURCE, Map.of("destinations", List.of(DEST_ONE, DEST_TWO)));

    deleteCopyFields(SOURCE, DEST_ONE);

    assertEquals(
        "only the named destination should have been removed",
        List.of(DEST_TWO),
        destinationsOf(SOURCE));
  }

  @Test
  public void testUnqualifiedDeleteRemovesEveryRuleForTheSource() throws Exception {
    addCopyFields(SOURCE, Map.of("destinations", List.of(DEST_ONE, DEST_TWO)));

    deleteCopyFields(SOURCE);

    assertEquals(List.of(), destinationsOf(SOURCE));
  }

  @Test
  public void testDeleteLeavesRulesWithOtherSourcesAlone() throws Exception {
    addCopyFields(SOURCE, Map.of("destinations", List.of(DEST_ONE)));

    deleteCopyFields(SOURCE);

    // 'id' -> 'id_prefix' is declared by the cloud-managed configset itself.
    assertEquals(List.of("id_prefix"), destinationsOf("id"));
  }

  @Test
  public void testDeleteOfUnknownSourceIsNotFound() throws Exception {
    final var thrown =
        expectThrows(RemoteSolrException.class, () -> deleteCopyFields("no_such_source_field"));

    assertEquals(404, thrown.code());
    assertTrue(
        "unexpected message: " + thrown.getMessage(),
        thrown.getMessage().contains("No copy-field rules found"));
  }

  @Test
  public void testAddRequiresDestinations() throws Exception {
    final var thrown =
        expectThrows(RemoteSolrException.class, () -> addCopyFields(SOURCE, new NamedList<>()));

    assertEquals(400, thrown.code());
    assertTrue(
        "unexpected message: " + thrown.getMessage(), thrown.getMessage().contains("destinations"));
  }
}
