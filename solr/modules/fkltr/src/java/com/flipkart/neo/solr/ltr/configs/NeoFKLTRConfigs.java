package com.flipkart.neo.solr.ltr.configs;

import com.flipkart.kloud.config.DynamicBucket;
import com.flipkart.neo.solr.ltr.constants.DynamicConfigBucketKeys;

public class NeoFKLTRConfigs {
  private final DynamicBucket dynamicBucket;

  public NeoFKLTRConfigs(DynamicBucket dynamicBucket) {
    this.dynamicBucket = dynamicBucket;
  }

  public boolean isPaginationEnabledForGroupQuery() {
    // false as of now.
    return false;
  }

  public boolean isPaginationEnabledForNonGroupQuery() {
    // false as of now.
    return false;
  }

  public boolean isIsMultiShardSetup() {
    // Keeping it in config as of now as retrieving from clusterState is expensive and tricky.
    return dynamicBucket.getBoolean(DynamicConfigBucketKeys.IS_MULTI_SHARD_SETUP);
  }

  public int getL1ParallelizationLevel() {
    return dynamicBucket.getInt(DynamicConfigBucketKeys.L1_PARALLELIZATION_LEVEL);
  }
}
