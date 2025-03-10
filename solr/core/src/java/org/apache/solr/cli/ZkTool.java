package org.apache.solr.cli;

import org.apache.solr.common.util.EnvUtils;
import picocli.CommandLine;

/**
 * Sub commands for working with ZooKeeper, only here to provide a common parent for the subcommands
 * and print tool help
 */
@CommandLine.Command(
    name = "zk",
    description = "Sub commands for working with ZooKeeper.",
    subcommands = {ZkLsTool.class})
public class ZkTool {
  public static class ZkHostDefaultValueProvider implements CommandLine.IDefaultValueProvider {
    @Override
    public String defaultValue(CommandLine.Model.ArgSpec argSpec) throws Exception {
      // NOCOMMIT: Need a more robust impl. Do we want one central provider or one per tool?
      return switch (argSpec.paramLabel()) {
        case "<zkHost>" -> EnvUtils.getProperty("zkHost");
        default -> null;
      };
    }
  }
}
