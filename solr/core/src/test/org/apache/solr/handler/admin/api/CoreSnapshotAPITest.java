package org.apache.solr.handler.admin.api;

import static org.mockito.Mockito.mock;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.CoreContainer;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class CoreSnapshotAPITest extends SolrTestCaseJ4 {

  private CoreContainer mockCoreContainer;

  private CoreSnapshotAPI coreSnapshotAPI;

  @BeforeClass
  public static void ensureWorkingMockito() {
    assumeWorkingMockito();
  }

  @Before
  public void setUp() throws Exception {
    super.setUp();

    mockCoreContainer = mock(CoreContainer.class);

    coreSnapshotAPI = new CoreSnapshotAPI(mockCoreContainer);
  }

  @Test
  public void testReportsErrorWhenCreatingSnapshotForNonexistentCore() {
    final String nonExistentCoreName = "non-existent";

    final SolrException solrException =
        expectThrows(
            SolrException.class,
            () -> {
              coreSnapshotAPI.createSnapshot(nonExistentCoreName, "my-new-snapshot");
            });
    assertEquals(400, solrException.code());
    assertTrue(
        "Exception message differed from expected: " + solrException.getMessage(),
        solrException.getMessage().contains("Unable to locate core " + nonExistentCoreName));
  }

  @Test
  public void testReportsErrorWhenListingSnapshotsForNonexistentCore() {
    final String nonExistentCoreName = "non-existent";

    final SolrException solrException =
        expectThrows(
            SolrException.class,
            () -> {
              coreSnapshotAPI.listSnapshots(nonExistentCoreName);
            });
    assertEquals(400, solrException.code());
    assertTrue(
        "Exception message differed from expected: " + solrException.getMessage(),
        solrException.getMessage().contains("Unable to locate core " + nonExistentCoreName));
  }

  @Test
  public void testReportsErrorWhenDeletingSnapshotForNonexistentCore() {
    final String nonExistentCoreName = "non-existent";

    final SolrException solrException =
        expectThrows(
            SolrException.class,
            () -> {
              coreSnapshotAPI.deleteSnapshot(nonExistentCoreName, "non-existent-snapshot");
            });
    assertEquals(400, solrException.code());
    assertTrue(
        "Exception message differed from expected: " + solrException.getMessage(),
        solrException.getMessage().contains("Unable to locate core " + nonExistentCoreName));
  }
}
