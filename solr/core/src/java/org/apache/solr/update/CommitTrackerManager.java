package org.apache.solr.update;

import java.lang.invoke.MethodHandles;
import java.util.Map;
import org.apache.solr.common.cloud.ClusterProperties;
import org.apache.solr.common.cloud.ZkStateReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * De-couple the commit tracker config management logic from CommitTracker instance. <br>
 * This only tracks and manage the ext.alignCommitTime override property for now.
 */
public class CommitTrackerManager {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  public static final String ALIGN_COMMIT_TIME_KEY =
      ClusterProperties.EXT_PROPRTTY_PREFIX + "alignCommitTime";
  private static volatile Boolean alignCommitTimeOverride;

  private CommitTrackerManager() {}

  public static void init(ZkStateReader zkStateReader) {
    zkStateReader.registerClusterPropertiesListener(
        (Map<String, Object> properties) -> {
          alignCommitTimeOverride = (Boolean) properties.get(ALIGN_COMMIT_TIME_KEY);
          log.info(
              "{} change detected serving alignCommitTimeOverride: {}",
              ALIGN_COMMIT_TIME_KEY,
              alignCommitTimeOverride);
          return false;
        });
  }

  public static Boolean getAlignCommitTimeOverride() {
    return alignCommitTimeOverride;
  }
}
