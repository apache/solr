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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.solr.client.api.model.AddCopyFieldRequest;
import org.apache.solr.client.api.model.CopyFieldDestination;
import org.apache.solr.client.api.model.DeleteCopyFieldRequest;
import org.apache.solr.client.api.model.IndexType;
import org.apache.solr.client.solrj.RemoteSolrException;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.client.solrj.request.SchemaApi;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.client.solrj.request.schema.SchemaRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.params.MapSolrParams;
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
    putCopyFields(SOURCE, rules());
  }

  private static String schemaPath(String suffix) {
    return "/collections/" + COLLECTION + "/schema" + suffix;
  }

  private static List<Map<String, String>> rules(String... destinations) {
    return Arrays.stream(destinations)
        .map(destination -> Map.of("destination", destination))
        .toList();
  }

  private static CopyFieldDestination destination(String field, Integer maxChars) {
    final var destination = new CopyFieldDestination();
    destination.destination = field;
    destination.maxChars = maxChars;
    return destination;
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
    return copyFields.stream().map(rule -> (String) rule.get("destination")).sorted().toList();
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
        .map(rule -> (String) rule.get("destination"))
        .sorted()
        .collect(Collectors.toList());
  }

  @Test
  public void testPutUsesIndividualDestinationLimits() throws Exception {
    putCopyFields(
        SOURCE,
        List.of(
            Map.of("destination", DEST_ONE, "maxChars", 100),
            Map.of("destination", DEST_TWO, "maxChars", 20)));

    var response =
        new V2Request.Builder(schemaPath("/copyfields/" + SOURCE))
            .GET()
            .build()
            .process(cluster.getSolrClient());
    assertEquals(
        List.of(
            Map.of("source", SOURCE, "destination", DEST_ONE, "maxChars", 100),
            Map.of("source", SOURCE, "destination", DEST_TWO, "maxChars", 20)),
        response.getResponse().get("copyFields"));

    var legacy = new SchemaRequest.CopyFields().process(cluster.getSolrClient(), COLLECTION);
    assertEquals(
        List.of(
            Map.of("source", SOURCE, "dest", DEST_ONE, "maxChars", 100),
            Map.of("source", SOURCE, "dest", DEST_TWO, "maxChars", 20)),
        ((List<?>) legacy.getResponse().get("copyFields"))
            .stream().filter(rule -> SOURCE.equals(((Map<?, ?>) rule).get("source"))).toList());
  }

  @Test
  public void testPostPreservesExistingLimitsAndAddsIndividualLimits() throws Exception {
    putCopyFields(SOURCE, List.of(Map.of("destination", DEST_ONE, "maxChars", 100)));
    postCopyFields(
        SOURCE,
        List.of(
            Map.of("destination", DEST_ONE, "maxChars", 1),
            Map.of("destination", DEST_TWO, "maxChars", 20)));
    var response =
        new SchemaApi.ListCopyFieldsBySource(IndexType.COLLECTION, COLLECTION, SOURCE)
            .process(cluster.getSolrClient());
    assertEquals(
        List.of(
            Map.of("source", SOURCE, "destination", DEST_ONE, "maxChars", 100),
            Map.of("source", SOURCE, "destination", DEST_TWO, "maxChars", 20)),
        response.copyFields);
  }

  @Test
  public void testPutChangesLimitsAndSupportsUnlimitedDestination() throws Exception {
    putCopyFields(SOURCE, List.of(Map.of("destination", DEST_ONE, "maxChars", 100)));
    putCopyFields(
        SOURCE,
        List.of(Map.of("destination", DEST_ONE, "maxChars", 20), Map.of("destination", DEST_TWO)));
    var response =
        new SchemaApi.ListCopyFieldsBySource(IndexType.COLLECTION, COLLECTION, SOURCE)
            .process(cluster.getSolrClient());
    assertEquals(
        List.of(
            Map.of("source", SOURCE, "destination", DEST_ONE, "maxChars", 20),
            Map.of("source", SOURCE, "destination", DEST_TWO)),
        response.copyFields);
  }

  @Test
  public void testFailedMultiDestinationPutAndPostAreAtomic() throws Exception {
    putCopyFields(SOURCE, List.of(Map.of("destination", DEST_ONE, "maxChars", 100)));
    var invalidRules =
        List.of(
            Map.of("destination", DEST_TWO, "maxChars", 20),
            Map.of("destination", "no_such_destination_field"));
    expectThrows(RemoteSolrException.class, () -> putCopyFields(SOURCE, invalidRules));
    expectThrows(RemoteSolrException.class, () -> postCopyFields(SOURCE, invalidRules));
    var response =
        new SchemaApi.ListCopyFieldsBySource(IndexType.COLLECTION, COLLECTION, SOURCE)
            .process(cluster.getSolrClient());
    assertEquals(
        List.of(Map.of("source", SOURCE, "destination", DEST_ONE, "maxChars", 100)),
        response.copyFields);
  }

  @Test
  public void testInvalidDestinationListsLeaveRulesIntact() throws Exception {
    putCopyFields(SOURCE, rules(DEST_ONE));
    var nullRule = new ArrayList<CopyFieldDestination>();
    nullRule.add(null);
    for (Object invalid :
        List.of(
            List.of(Map.of("destination", DEST_TWO, "maxChars", -1)),
            List.of(Map.of("destination", "")),
            List.of(Map.of("destination", " ")),
            rules(DEST_TWO, DEST_TWO),
            nullRule,
            List.of(Map.of("maxChars", 10)),
            List.of(Map.of("destinations", List.of(DEST_TWO))))) {
      assertEquals(
          400,
          expectThrows(RemoteSolrException.class, () -> putCopyFields(SOURCE, invalid)).code());
      assertEquals(
          400,
          expectThrows(RemoteSolrException.class, () -> postCopyFields(SOURCE, invalid)).code());
      assertEquals(List.of(DEST_ONE), destinationsOf(SOURCE));
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testV2SchemaReadsUseDestinationAndV1RetainsDest() throws Exception {
    new SchemaRequest.AddCopyField(SOURCE, List.of(DEST_ONE, DEST_TWO), 100)
        .process(cluster.getSolrClient(), COLLECTION);
    var v2Schema =
        new SchemaApi.GetSchemaInfo(IndexType.COLLECTION, COLLECTION)
            .process(cluster.getSolrClient());
    var v2Rules = (List<Map<String, Object>>) v2Schema.schema.get("copyFields");
    assertTrue(
        v2Rules.stream()
            .allMatch(rule -> rule.containsKey("destination") && !rule.containsKey("dest")));
    var v1Schema = new SchemaRequest().process(cluster.getSolrClient(), COLLECTION);
    var v1Rules =
        (List<Map<String, Object>>)
            ((Map<?, ?>) v1Schema.getResponse().get("schema")).get("copyFields");
    assertTrue(
        v1Rules.stream()
            .allMatch(rule -> rule.containsKey("dest") && !rule.containsKey("destination")));
    var v2Filtered =
        new V2Request.Builder(schemaPath("/copyfields"))
            .GET()
            .withParams(params("source.fl", SOURCE, "destination.fl", DEST_TWO))
            .build()
            .process(cluster.getSolrClient());
    assertEquals(
        List.of(Map.of("source", SOURCE, "destination", DEST_TWO, "maxChars", 100)),
        v2Filtered.getResponse().get("copyFields"));
    var v1Filtered =
        new SchemaRequest.CopyFields(params("source.fl", SOURCE, "dest.fl", DEST_TWO))
            .process(cluster.getSolrClient(), COLLECTION);
    assertEquals(
        List.of(Map.of("source", SOURCE, "dest", DEST_TWO, "maxChars", 100)),
        v1Filtered.getCopyFields());
    new SchemaRequest.DeleteCopyField(SOURCE, List.of(DEST_ONE, DEST_TWO))
        .process(cluster.getSolrClient(), COLLECTION);
    assertEquals(List.of(), destinationsOf(SOURCE));
  }

  @Test
  public void testV1StillAcceptsScalarDest() throws Exception {
    var request =
        new GenericSolrRequest(SolrRequest.METHOD.POST, "/schema")
            .setRequiresCollection(true)
            .withContent(
                ("{\"add-copy-field\":{\"source\":\""
                        + SOURCE
                        + "\",\"dest\":\""
                        + DEST_ONE
                        + "\",\"maxChars\":20}}")
                    .getBytes(StandardCharsets.UTF_8),
                "application/json");
    request.process(cluster.getSolrClient(), COLLECTION);
    assertEquals(
        List.of(Map.of("source", SOURCE, "dest", DEST_ONE, "maxChars", 20)),
        new SchemaRequest.CopyFields(params("source.fl", SOURCE))
            .process(cluster.getSolrClient(), COLLECTION)
            .getCopyFields());
  }

  @Test
  public void testV2BulkUsesOneDestinationPerOperation() throws Exception {
    var bulk = new SchemaApi.BulkSchemaModification(IndexType.COLLECTION, COLLECTION);
    for (var rule : List.of(destination(DEST_ONE, 100), destination(DEST_TWO, 20))) {
      var add = new AddCopyFieldRequest();
      add.operationType = "add-copy-field";
      add.source = SOURCE;
      add.destination = rule.destination;
      add.maxChars = rule.maxChars;
      bulk.addSchemaChange(add);
    }
    bulk.process(cluster.getSolrClient());
    var response =
        new SchemaApi.ListCopyFieldsBySource(IndexType.COLLECTION, COLLECTION, SOURCE)
            .process(cluster.getSolrClient());
    assertEquals(
        List.of(
            Map.of("source", SOURCE, "destination", DEST_ONE, "maxChars", 100),
            Map.of("source", SOURCE, "destination", DEST_TWO, "maxChars", 20)),
        response.copyFields);
    var delete = new DeleteCopyFieldRequest();
    delete.operationType = "delete-copy-field";
    delete.source = SOURCE;
    delete.destination = DEST_ONE;
    var deleteBulk = new SchemaApi.BulkSchemaModification(IndexType.COLLECTION, COLLECTION);
    deleteBulk.addSchemaChange(delete);
    deleteBulk.process(cluster.getSolrClient());
    assertEquals(List.of(DEST_TWO), destinationsOf(SOURCE));
    for (String legacyName : List.of("dest", "destinations")) {
      assertEquals(
          400,
          expectThrows(
                  RemoteSolrException.class,
                  () ->
                      new V2Request.Builder(schemaPath("/bulk"))
                          .POST()
                          .withPayload(
                              List.of(
                                  Map.of(
                                      "operationType",
                                      "add-copy-field",
                                      "source",
                                      SOURCE,
                                      legacyName,
                                      DEST_ONE)))
                          .build()
                          .process(cluster.getSolrClient()))
              .code());
    }
    assertEquals(List.of(DEST_TWO), destinationsOf(SOURCE));
  }

  @Test
  public void testDynamicSourceSupportsIndividualLimits() throws Exception {
    String source = "*_s";
    try {
      putCopyFields(
          source,
          List.of(
              Map.of("destination", DEST_ONE, "maxChars", 100),
              Map.of("destination", DEST_TWO, "maxChars", 20)));
      var response =
          new SchemaApi.ListCopyFieldsBySource(IndexType.COLLECTION, COLLECTION, source)
              .process(cluster.getSolrClient());
      assertEquals(
          List.of(
              Map.of("source", source, "destination", DEST_ONE, "maxChars", 100),
              Map.of("source", source, "destination", DEST_TWO, "maxChars", 20)),
          response.copyFields);
      putCopyFields(source, List.of(Map.of("destination", DEST_TWO, "maxChars", 0)));
      postCopyFields(source, List.of());
      response =
          new SchemaApi.ListCopyFieldsBySource(IndexType.COLLECTION, COLLECTION, source)
              .process(cluster.getSolrClient());
      assertEquals(List.of(Map.of("source", source, "destination", DEST_TWO)), response.copyFields);
    } finally {
      putCopyFields(source, List.of());
    }
  }

  @Test
  public void testAddAndListCopyFieldsBySource() throws Exception {
    putCopyFields(SOURCE, rules(DEST_ONE, DEST_TWO));

    assertEquals(List.of(DEST_ONE, DEST_TWO), destinationsOf(SOURCE));
  }

  @Test
  public void testGeneratedPutReplacesCopyFields() throws Exception {
    putCopyFields(SOURCE, rules(DEST_ONE, DEST_TWO));

    var request = new SchemaApi.UpsertCopyFields(IndexType.COLLECTION, COLLECTION, SOURCE);
    request.addCopyFieldDestination(destination(DEST_TWO, 100));
    assertEquals(0, request.process(cluster.getSolrClient()).responseHeader.status);

    var response =
        new SchemaApi.ListCopyFieldsBySource(IndexType.COLLECTION, COLLECTION, SOURCE)
            .process(cluster.getSolrClient());
    assertEquals(
        List.of(Map.of("source", SOURCE, "destination", DEST_TWO, "maxChars", 100)),
        response.copyFields);
  }

  @Test
  public void testGeneratedPostAppendsCopyFields() throws Exception {
    putCopyFields(SOURCE, rules(DEST_ONE));

    var request = new SchemaApi.AppendCopyFields(IndexType.COLLECTION, COLLECTION, SOURCE);
    request.addCopyFieldDestination(destination(DEST_TWO, 100));
    assertEquals(0, request.process(cluster.getSolrClient()).responseHeader.status);

    var response =
        new SchemaApi.ListCopyFieldsBySource(IndexType.COLLECTION, COLLECTION, SOURCE)
            .process(cluster.getSolrClient());
    assertEquals(
        List.of(
            Map.of("source", SOURCE, "destination", DEST_ONE),
            Map.of("source", SOURCE, "destination", DEST_TWO, "maxChars", 100)),
        response.copyFields);
  }

  @Test
  public void testRepeatedPutIsIdempotent() throws Exception {
    putCopyFields(SOURCE, rules(DEST_ONE, DEST_TWO));
    putCopyFields(SOURCE, rules(DEST_ONE, DEST_TWO));

    // 'add-copy-field' on its own would append a second, duplicate rule for each destination,
    // making the source get copied twice over at index time.
    assertEquals(List.of(DEST_ONE, DEST_TWO), destinationsOf(SOURCE));
  }

  @Test
  public void testPutReplacesTheSourcesExistingRules() throws Exception {
    putCopyFields(SOURCE, rules(DEST_ONE, DEST_TWO));

    putCopyFields(SOURCE, rules(DEST_TWO));

    assertEquals(
        "destinations left out of the request should be gone",
        List.of(DEST_TWO),
        destinationsOf(SOURCE));
  }

  @Test
  public void testPutCanExtendTheSourcesExistingRules() throws Exception {
    putCopyFields(SOURCE, rules(DEST_ONE));

    putCopyFields(SOURCE, rules(DEST_ONE, DEST_TWO));

    assertEquals(List.of(DEST_ONE, DEST_TWO), destinationsOf(SOURCE));
  }

  @Test
  public void testFailedPutLeavesExistingRulesIntact() throws Exception {
    putCopyFields(SOURCE, rules(DEST_ONE));

    expectThrows(
        RemoteSolrException.class, () -> putCopyFields(SOURCE, rules("no_such_destination_field")));

    assertEquals(
        "a rejected replacement must not drop the rules it would have replaced",
        List.of(DEST_ONE),
        destinationsOf(SOURCE));
  }

  @Test
  public void testAddAcceptsSingleDestinationAndMaxChars() throws Exception {
    putCopyFields(SOURCE, List.of(Map.of("destination", DEST_ONE, "maxChars", 100)));

    assertEquals(List.of(DEST_ONE), destinationsOf(SOURCE));
  }

  @Test
  public void testDeleteNarrowedByDestinationParam() throws Exception {
    putCopyFields(SOURCE, rules(DEST_ONE, DEST_TWO));

    deleteCopyFields(SOURCE, DEST_ONE);

    assertEquals(
        "only the named destination should have been removed",
        List.of(DEST_TWO),
        destinationsOf(SOURCE));
  }

  @Test
  public void testUnqualifiedDeleteRemovesEveryRuleForTheSource() throws Exception {
    putCopyFields(SOURCE, rules(DEST_ONE, DEST_TWO));

    deleteCopyFields(SOURCE);

    assertEquals(List.of(), destinationsOf(SOURCE));
  }

  @Test
  public void testDeleteLeavesRulesWithOtherSourcesAlone() throws Exception {
    putCopyFields(SOURCE, rules(DEST_ONE));

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
    putCopyFields(SOURCE, rules(DEST_ONE, DEST_TWO));

    assertEquals(List.of(DEST_ONE, DEST_TWO), destinationsViaSourceEndpoint(SOURCE));
    assertEquals(List.of("id_prefix"), destinationsViaSourceEndpoint("id"));
  }

  @Test
  public void testGetBySourceIsEmptyForASourceWithNoRules() throws Exception {
    assertEquals(List.of(), destinationsViaSourceEndpoint("no_such_source_field"));
  }

  @Test
  public void testPostAppendsWithoutDisturbingExistingRules() throws Exception {
    putCopyFields(SOURCE, rules(DEST_ONE));

    postCopyFields(SOURCE, rules(DEST_TWO));

    assertEquals(List.of(DEST_ONE, DEST_TWO), destinationsOf(SOURCE));
  }

  @Test
  public void testPostSkipsDestinationsAlreadyPresent() throws Exception {
    putCopyFields(SOURCE, rules(DEST_ONE));

    postCopyFields(SOURCE, rules(DEST_ONE, DEST_TWO));
    postCopyFields(SOURCE, rules(DEST_ONE, DEST_TWO));

    // A repeated append must not leave the source copied twice over.
    assertEquals(List.of(DEST_ONE, DEST_TWO), destinationsOf(SOURCE));
  }

  @Test
  public void testDeleteOfSeveralDestinationsInOnePathSegment() throws Exception {
    putCopyFields(SOURCE, rules(DEST_ONE, DEST_TWO));

    deleteCopyFields(SOURCE, DEST_ONE, DEST_TWO);

    assertEquals(List.of(), destinationsOf(SOURCE));
  }

  @Test
  public void testDeleteOfMissingNamedRuleIsNotFound() throws Exception {
    putCopyFields(SOURCE, rules(DEST_ONE));

    var thrown = expectThrows(RemoteSolrException.class, () -> deleteCopyFields(SOURCE, DEST_TWO));

    assertEquals(404, thrown.code());
    assertTrue(thrown.getMessage(), thrown.getMessage().contains("Copy field directive not found"));
    assertEquals(List.of(DEST_ONE), destinationsOf(SOURCE));
  }

  @Test
  public void testDeleteOfNamedRuleWithUnknownSourceIsNotFound() throws Exception {
    var thrown =
        expectThrows(
            RemoteSolrException.class, () -> deleteCopyFields("no_such_source_field", DEST_ONE));

    assertEquals(404, thrown.code());
  }

  @Test
  public void testDeleteOfMissingNamedRuleLeavesOtherRulesIntact() throws Exception {
    putCopyFields(SOURCE, rules(DEST_ONE));

    var thrown =
        expectThrows(RemoteSolrException.class, () -> deleteCopyFields(SOURCE, DEST_ONE, DEST_TWO));

    assertEquals(404, thrown.code());
    assertEquals(List.of(DEST_ONE), destinationsOf(SOURCE));
  }

  @Test
  public void testDeleteOfInvalidDestinationGlobIsBadRequest() throws Exception {
    putCopyFields(SOURCE, rules(DEST_ONE));

    var thrown =
        expectThrows(RemoteSolrException.class, () -> deleteCopyFields(SOURCE, "invalid**glob"));

    assertEquals(400, thrown.code());
    assertEquals(List.of(DEST_ONE), destinationsOf(SOURCE));
  }

  @Test
  public void testBulkDeleteOfMissingRuleIsBadRequest() throws Exception {
    var thrown =
        expectThrows(
            RemoteSolrException.class,
            () ->
                new V2Request.Builder(schemaPath("/bulk"))
                    .POST()
                    .withPayload(
                        List.of(
                            Map.of(
                                "operationType",
                                "delete-copy-field",
                                "source",
                                SOURCE,
                                "destination",
                                DEST_TWO)))
                    .build()
                    .process(cluster.getSolrClient()));

    assertEquals(400, thrown.code());
  }

  @Test
  public void testV1DeleteOfMissingRuleIsBadRequest() {
    var thrown =
        expectThrows(
            RemoteSolrException.class,
            () ->
                new SchemaRequest.DeleteCopyField(SOURCE, List.of(DEST_TWO))
                    .process(cluster.getSolrClient(), COLLECTION));

    assertEquals(400, thrown.code());
  }

  @Test
  public void testPutWithNoDestinationsClearsTheSource() throws Exception {
    putCopyFields(SOURCE, rules(DEST_ONE, DEST_TWO));

    putCopyFields(SOURCE, rules());

    assertEquals(List.of(), destinationsOf(SOURCE));
  }

  @Test
  public void testDeleteRejectsAPathSegmentNamingNoDestinations() throws Exception {
    putCopyFields(SOURCE, rules(DEST_ONE));

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
  public void testV2RejectsLegacyDestinationName() {
    final var thrown =
        expectThrows(
            RemoteSolrException.class,
            () -> putCopyFields(SOURCE, List.of(Map.of("dest", DEST_ONE))));

    assertEquals(400, thrown.code());
  }

  @Test
  public void testAddRequiresDestinations() throws Exception {
    final var thrown =
        expectThrows(
            RemoteSolrException.class,
            () -> putCopyFields(SOURCE, List.of(Map.of("maxChars", 100))));

    assertEquals(400, thrown.code());
    assertTrue(
        "unexpected message: " + thrown.getMessage(), thrown.getMessage().contains("destination"));
  }
}
