package org.apache.solr.security;

import java.util.Arrays;
import java.util.Map;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.util.Utils;
import org.junit.After;
import org.junit.Test;

/**
 * This tries to ensure that our test framework reliably mimics the real world with regard to
 * Authorization, even though our tests use the same JVM for both client and server.
 *
 * <p>NOCOMMIT: This test takes a few seconds to run, adds little value, and is for demonstration
 * purposes only. We could reverse the order in CloudAuthStreamTest#testSanityCheckAuth and get the
 * same coverage.
 */
public class TestAuthorizationWhenClientInSameJvm extends SolrCloudTestCase {

  @Test
  public void testReadOnlyUserCannotUpdate() {
    // This fails because of a test framework bug.
    // When CloudSolrClient is used with parallel updates turned on,
    // with the client and server both running in the same JVM
    // The PKI Authentication Plugin gets confused and thinks our update
    // it is an inter-node request.

    // Setting up the cluster with parallel updates enabled (the default)
    setupCluster(true);

    // we attempt to add a document with an *UN-authorized* user.
    addDocument(USER_READ_ONLY, USER_READ_ONLY_PW, "1-not-allowed-from-read-only-user");

    // we commit with an *authorized* user.
    commit(USER_CAN_UPDATE, USER_CAN_UPDATE_PW);

    // we expect there to be no documents.
    assertDocumentsInCollection(0);
  }

  @Test
  public void testReadOnlyUserCannotUpdate_disableParallelUpdates() {
    // This averts the bug by disabling parallel updates on CloudSolrClient.
    setupCluster(false);

    // we attempt to add a document with an *UN-authorized* user.
    addDocument(USER_READ_ONLY, USER_READ_ONLY_PW, "1-not-allowed-from-read-only-user");

    // we commit with an *authorized* user.
    commit(USER_CAN_UPDATE, USER_CAN_UPDATE_PW);

    // we expect there to be no documents.
    assertDocumentsInCollection(0);
  }

  @Test
  public void testReadOnlyUserCannotCommit() {
    // This does not fail because parallel updates are not
    // used by commit requests.

    // Setting up the cluster with parallel updates enabled (the default)
    // This does not fail because parallel updates are not
    // used by commit requests.
    setupCluster(true);

    // we add a document with an *authorized* user.
    addDocument(USER_CAN_UPDATE, USER_CAN_UPDATE_PW, "2-allowed-from-authorized-user");

    // we commit with an *UN-authorized* user.
    commit(USER_READ_ONLY, USER_READ_ONLY_PW);

    // we expect there to be no documents.
    assertDocumentsInCollection(0);
  }

  @Test
  public void testDocumentExistsIfBothAddAndCommitAreAuthorized() {
    // Setting up the cluster with parallel updates enabled (the default)
    setupCluster(true);

    // we add a document with an *authorized* user.
    addDocument(USER_CAN_UPDATE, USER_CAN_UPDATE_PW, "3-allowed-from-authorized-user");

    // we commit with an *authorized* user.
    commit(USER_CAN_UPDATE, USER_CAN_UPDATE_PW);

    // we expect there to be 1 document.
    assertDocumentsInCollection(1);
  }

  private void addDocument(String user, String password, String id) {
    UpdateRequest update = new UpdateRequest();
    update.setBasicAuthCredentials(user, password);
    update.add("id", id);
    try {
      cluster.getSolrClient().request(update, COLLECTION_NAME);
    } catch (Exception e) {
      // ignore, we will validate by counting documents
    }
  }

  private void commit(String user, String password) {
    UpdateRequest commit = new UpdateRequest();
    commit.setBasicAuthCredentials(user, password);
    try {
      commit.commit(cluster.getSolrClient(), COLLECTION_NAME);
    } catch (Exception e) {
      // ignore, we will validate by counting documents
    }
  }

  private void assertDocumentsInCollection(int expected) {
    QueryResponse resp;
    try {
      QueryRequest req = new QueryRequest(new MapSolrParams(Map.of("q", "*:*")));
      req.setBasicAuthCredentials(USER_READ_ONLY, USER_READ_ONLY_PW);
      resp = req.process(cluster.getSolrClient(), COLLECTION_NAME);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    String failMessage =
        resp.getResults().size() > 0
            ? "first id: " + resp.getResults().iterator().next().get("id")
            : "";
    assertEquals(failMessage, expected, resp.getResults().getNumFound());
  }

  @Override
  @After
  public void tearDown() throws Exception {
    cluster.shutdown();
    super.tearDown();
  }

  private void setupCluster(boolean parallelUpdates) {
    // Important:  "cloud-minimal" does not configure any autocommit!
    // Otherwise, checking if unauthorized users could commit would be timing-specific.
    try {
      configureCluster(1)
          .addConfig("conf", configset("cloud-minimal"))
          .withSecurityJson(securityJson())
          .withClientParallelUpdates(parallelUpdates) // this defaults to 'true'
          .configure();
      CollectionAdminRequest.createCollection(COLLECTION_NAME, "conf", 1, 1)
          .setBasicAuthCredentials(USER_CAN_UPDATE, USER_CAN_UPDATE_PW)
          .process(cluster.getSolrClient());
      cluster.waitForActiveCollection(COLLECTION_NAME, 1, 1);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static final String COLLECTION_NAME = "the_collection";
  private static final String ROLE_UPDATE = "update_role";
  private static final String USER_CAN_UPDATE = "update_user";
  private static final String USER_CAN_UPDATE_PW = "update_user_PW";
  private static final String USER_READ_ONLY = "read_only_user";
  private static final String USER_READ_ONLY_PW = "read_only_user_pw";

  private String securityJson() {
    return Utils.toJSONString(
        Map.of(
            "authorization",
            Map.of(
                "class",
                RuleBasedAuthorizationPlugin.class.getName(),
                "user-role",
                Map.of(USER_CAN_UPDATE, ROLE_UPDATE),
                "permissions",
                Arrays.asList(
                    Map.of("name", "read", "role", "*"),
                    Map.of("name", "all", "role", ROLE_UPDATE))),
            "authentication",
            Map.of(
                "class",
                BasicAuthPlugin.class.getName(),
                "blockUnknown",
                true,
                "credentials",
                Map.of(
                    USER_CAN_UPDATE,
                    Sha256AuthenticationProvider.getSaltedHashedValue(USER_CAN_UPDATE_PW),
                    USER_READ_ONLY,
                    Sha256AuthenticationProvider.getSaltedHashedValue(USER_READ_ONLY_PW)))));
  }
}
