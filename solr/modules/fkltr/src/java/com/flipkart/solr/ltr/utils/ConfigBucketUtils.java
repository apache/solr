package com.flipkart.solr.ltr.utils;

import com.flipkart.kloud.config.DynamicBucket;

public class ConfigBucketUtils {
  public static boolean getBoolean(DynamicBucket dynamicBucket, String key, boolean defaultValue) {
    Boolean value = dynamicBucket.getBoolean(key);
    return value == null ? defaultValue : value;
  }
}
