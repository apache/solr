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
package org.apache.solr.jersey;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CollectionsApi;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.util.SecurityJson;
import org.junit.Test;

/**
 * Reproduction/regression coverage for SOLR-18324.
 *
 * <p>Hammers {@code CollectionsApi.GetCollectionStatus} against a freshly created single-replica
 * collection on a 2-node cluster, which occasionally races with inter-node request handling before
 * {@code V2HttpCall} has attached a {@code SolrQueryRequest} to the Jersey request context. Before
 * this fix, that race cascaded into the bug described in SOLR-18324: a second NPE while handling
 * the first, then an {@code AssertionError} in {@code RTimer.stop()} (only visible with assertions
 * enabled, e.g. under Gradle's test JVM). Against {@code main} prior to this fix, this reproduced
 * at a substantial rate in local runs -- ~39/100 (39%) attempts against a {@link
 * SecurityJson#SIMPLE}-secured cluster, and ~96/200 (48%) against an equivalent unsecured cluster.
 * That second data point matters: an earlier draft of this investigation assumed the failure was
 * specific to basic-auth-secured clusters, but it reproduces at least as often with no security
 * configured at all -- it's a race in inter-node V2/Jersey request handling, independent of
 * authentication.
 *
 * <p>The underlying race that produces the <em>original</em> exception (before this fix's
 * null-checks ever run) isn't itself root-caused or fixed here, so this doesn't assert on zero
 * failures -- only that the failure rate stays far below the ~40-50% seen pre-fix. With the fix
 * applied, local runs consistently see 0/{@value #ATTEMPTS}; {@link #MAX_ACCEPTABLE_FAILURES}
 * leaves headroom for incidental, unrelated flakiness without masking a regression of this specific
 * bug.
 */
public class GetCollectionStatusNpeReproTest extends SolrCloudTestCase {

  private static final String COLLECTION = "reproColl";
  private static final int ATTEMPTS = 100;
  private static final int MAX_ACCEPTABLE_FAILURES = 5;

  private <T extends SolrRequest<? extends SolrResponse>> T withBasicAuth(T req) {
    req.setBasicAuthCredentials(SecurityJson.USER, SecurityJson.PASS);
    return req;
  }

  @Test
  public void reproUnderBasicAuth() throws Exception {
    configureCluster(2)
        .addConfig("conf", configset("cloud-minimal"))
        .withSecurityJson(SecurityJson.SIMPLE)
        .configure();
    try {
      withBasicAuth(CollectionAdminRequest.createCollection(COLLECTION, "conf", 1, 1))
          .processAndWait(cluster.getSolrClient(), 10);
      waitForState("expected collection", COLLECTION, clusterShape(1, 1));

      assertFailureRateBelowThreshold(hammerGetCollectionStatus(true));
    } finally {
      shutdownCluster();
    }
  }

  @Test
  public void reproWithoutSecurity() throws Exception {
    configureCluster(2).addConfig("conf", configset("cloud-minimal")).configure();
    try {
      CollectionAdminRequest.createCollection(COLLECTION, "conf", 1, 1)
          .processAndWait(cluster.getSolrClient(), 10);
      waitForState("expected collection", COLLECTION, clusterShape(1, 1));

      assertFailureRateBelowThreshold(hammerGetCollectionStatus(false));
    } finally {
      shutdownCluster();
    }
  }

  private void assertFailureRateBelowThreshold(int failures) {
    assertTrue(
        "Expected at most "
            + MAX_ACCEPTABLE_FAILURES
            + "/"
            + ATTEMPTS
            + " GetCollectionStatus calls to fail, but saw "
            + failures
            + " -- check the server log for an AssertionError in RTimer.stop(), which would mean"
            + " the SOLR-18324 cascade has regressed.",
        failures <= MAX_ACCEPTABLE_FAILURES);
  }

  private int hammerGetCollectionStatus(boolean withAuth) {
    int failures = 0;
    for (int i = 0; i < ATTEMPTS; i++) {
      try {
        var req = new CollectionsApi.GetCollectionStatus(COLLECTION);
        if (withAuth) {
          req.setBasicAuthCredentials(SecurityJson.USER, SecurityJson.PASS);
        }
        req.process(cluster.getSolrClient());
      } catch (Throwable t) {
        failures++;
      }
    }
    return failures;
  }
}
