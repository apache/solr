package org.apache.solr.core;

import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;
import org.apache.solr.cloud.ZkController;

public class SyntheticCoreDescriptor extends CoreDescriptor {
  public SyntheticCoreDescriptor(
      String syntheticCoreName,
      Path path,
      Map<String, String> coreProps,
      Properties containerProperties,
      ZkController zkController) {
    super(syntheticCoreName, path, coreProps, containerProperties, zkController);
  }
}
