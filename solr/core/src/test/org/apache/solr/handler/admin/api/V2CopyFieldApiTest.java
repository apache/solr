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
    // A replace with no destinations clears the source whether or not it currently has rules,
    // so unlike DELETE this needs no special case for the empty state.
    putCopyFields(SOURCE, Map.of("destinations", List.of()));
  }

  private static String schemaPath(String suffix) {
    return "/collections/" + COLLECTION + "/schema" + suffix;
  }

  private static void putCopyFields(String source, Object payload) throws Exception {
    new V2Request.Builder(schemaPath("/copyfields/" + source))
        .PUT()
        .withPayload(payload)
        .build()
        .process(cluster.getSolrClient());
  }

  private static void deleteCopyFields(String source, String... destinations) throws Exception {
    final var suffix = destinations.length == 0 ? "" : "/" + String.join(",", destinations);
    new V2Request.Builder(schemaPath("/copyfields/" + source + suffix))
        .DELETE()
        .build()
        .process(cluster.getSolrClient());
  }

  private static void postCopyFields(String source, Object payload) throws Exception {
    new V2Request.Builder(schemaPath("/copyfields/" + source))
        .POST()
        .withPayload(payload)
        .build()
        .process(cluster.getSolrClient());
  }

  /** Destinations reported by the per-source GET endpoint. */
  @SuppressWarnings("unchecked")
  private static List<String> destinationsViaSourceEndpoint(String source) throws Exception {
    final var response =
        new V2Request.Builder(schemaPath("/copyfields/" + source))
            .GET()
            .build()
            .process(cluster.getSolrClient());
    final var copyFields = (List<Map<String, Object>>) response.getResponse().get("copyFields");
    return copyFields.stream().map(rule -> (String) rule.get("dest")).sorted().toList();
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
    putCopyFields(SOURCE, Map.of("destinations", List.of(DEST_ONE, DEST_TWO)));

    assertEquals(List.of(DEST_ONE, DEST_TWO), destinationsOf(SOURCE));
  }

  @Test
  public void testRepeatedPutIsIdempotent() throws Exception {
    putCopyFields(SOURCE, Map.of("destinations", List.of(DEST_ONE, DEST_TWO)));
    putCopyFields(SOURCE, Map.of("destinations", List.of(DEST_ONE, DEST_TWO)));

    // 'add-copy-field' on its own would append a second, duplicate rule for each destination,
    // making the source get copied twice over at index time.
    assertEquals(List.of(DEST_ONE, DEST_TWO), destinationsOf(SOURCE));
  }

  @Test
  public void testPutReplacesTheSourcesExistingRules() throws Exception {
    putCopyFields(SOURCE, Map.of("destinations", List.of(DEST_ONE, DEST_TWO)));

    putCopyFields(SOURCE, Map.of("destinations", List.of(DEST_TWO)));

    assertEquals(
        "destinations left out of the request should be gone",
        List.of(DEST_TWO),
        destinationsOf(SOURCE));
  }

  @Test
  public void testPutCanExtendTheSourcesExistingRules() throws Exception {
    putCopyFields(SOURCE, Map.of("destinations", List.of(DEST_ONE)));

    putCopyFields(SOURCE, Map.of("destinations", List.of(DEST_ONE, DEST_TWO)));

    assertEquals(List.of(DEST_ONE, DEST_TWO), destinationsOf(SOURCE));
  }

  @Test
  public void testFailedPutLeavesExistingRulesIntact() throws Exception {
    putCopyFields(SOURCE, Map.of("destinations", List.of(DEST_ONE)));

    expectThrows(
        RemoteSolrException.class,
        () -> putCopyFields(SOURCE, Map.of("destinations", List.of("no_such_destination_field"))));

    assertEquals(
        "a rejected replacement must not drop the rules it would have replaced",
        List.of(DEST_ONE),
        destinationsOf(SOURCE));
  }

  @Test
  public void testAddAcceptsSingleDestinationAndMaxChars() throws Exception {
    putCopyFields(SOURCE, Map.of("destinations", DEST_ONE, "maxChars", 100));

    assertEquals(List.of(DEST_ONE), destinationsOf(SOURCE));
  }

  @Test
  public void testDeleteNarrowedByDestinationParam() throws Exception {
    putCopyFields(SOURCE, Map.of("destinations", List.of(DEST_ONE, DEST_TWO)));

    deleteCopyFields(SOURCE, DEST_ONE);

    assertEquals(
        "only the named destination should have been removed",
        List.of(DEST_TWO),
        destinationsOf(SOURCE));
  }

  @Test
  public void testUnqualifiedDeleteRemovesEveryRuleForTheSource() throws Exception {
    putCopyFields(SOURCE, Map.of("destinations", List.of(DEST_ONE, DEST_TWO)));

    deleteCopyFields(SOURCE);

    assertEquals(List.of(), destinationsOf(SOURCE));
  }

  @Test
  public void testDeleteLeavesRulesWithOtherSourcesAlone() throws Exception {
    putCopyFields(SOURCE, Map.of("destinations", List.of(DEST_ONE)));

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
  public void testGetBySourceReturnsOnlyThatSourcesDestinations() throws Exception {
    putCopyFields(SOURCE, Map.of("destinations", List.of(DEST_ONE, DEST_TWO)));

    assertEquals(List.of(DEST_ONE, DEST_TWO), destinationsViaSourceEndpoint(SOURCE));
    assertEquals(List.of("id_prefix"), destinationsViaSourceEndpoint("id"));
  }

  @Test
  public void testGetBySourceIsEmptyForASourceWithNoRules() throws Exception {
    assertEquals(List.of(), destinationsViaSourceEndpoint("no_such_source_field"));
  }

  @Test
  public void testPostAppendsWithoutDisturbingExistingRules() throws Exception {
    putCopyFields(SOURCE, Map.of("destinations", List.of(DEST_ONE)));

    postCopyFields(SOURCE, Map.of("destinations", List.of(DEST_TWO)));

    assertEquals(List.of(DEST_ONE, DEST_TWO), destinationsOf(SOURCE));
  }

  @Test
  public void testPostSkipsDestinationsAlreadyPresent() throws Exception {
    putCopyFields(SOURCE, Map.of("destinations", List.of(DEST_ONE)));

    postCopyFields(SOURCE, Map.of("destinations", List.of(DEST_ONE, DEST_TWO)));
    postCopyFields(SOURCE, Map.of("destinations", List.of(DEST_ONE, DEST_TWO)));

    // A repeated append must not leave the source copied twice over.
    assertEquals(List.of(DEST_ONE, DEST_TWO), destinationsOf(SOURCE));
  }

  @Test
  public void testDeleteOfSeveralDestinationsInOnePathSegment() throws Exception {
    putCopyFields(SOURCE, Map.of("destinations", List.of(DEST_ONE, DEST_TWO)));

    deleteCopyFields(SOURCE, DEST_ONE, DEST_TWO);

    assertEquals(List.of(), destinationsOf(SOURCE));
  }

  @Test
  public void testPutWithNoDestinationsClearsTheSource() throws Exception {
    putCopyFields(SOURCE, Map.of("destinations", List.of(DEST_ONE, DEST_TWO)));

    putCopyFields(SOURCE, Map.of("destinations", List.of()));

    assertEquals(List.of(), destinationsOf(SOURCE));
  }

  @Test
  public void testDeleteRejectsAPathSegmentNamingNoDestinations() throws Exception {
    putCopyFields(SOURCE, Map.of("destinations", List.of(DEST_ONE)));

    final var thrown =
        expectThrows(
            RemoteSolrException.class,
            () ->
                new V2Request.Builder(schemaPath("/copyfields/" + SOURCE + "/,,,"))
                    .DELETE()
                    .build()
                    .process(cluster.getSolrClient()));

    // The source does have rules, so this is a malformed request rather than a missing resource.
    assertEquals(400, thrown.code());
    assertEquals(List.of(DEST_ONE), destinationsOf(SOURCE));
  }

  @Test
  public void testBodySourceContradictingThePathIsRejected() {
    final var thrown =
        expectThrows(
            RemoteSolrException.class,
            () ->
                putCopyFields(
                    SOURCE,
                    Map.of("source", "some_other_source", "destinations", List.of(DEST_ONE))));

    assertEquals(400, thrown.code());
    assertTrue(
        "unexpected message: " + thrown.getMessage(),
        thrown.getMessage().contains("does not match the source in the path"));
  }

  @Test
  public void testAddRequiresDestinations() throws Exception {
    final var thrown =
        expectThrows(RemoteSolrException.class, () -> putCopyFields(SOURCE, new NamedList<>()));

    assertEquals(400, thrown.code());
    assertTrue(
        "unexpected message: " + thrown.getMessage(), thrown.getMessage().contains("destinations"));
  }
}
