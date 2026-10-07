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
package org.apache.solr.cli;

import java.util.ArrayList;
import java.util.List;
import org.apache.solr.logging.DeprecationLog;
import org.apache.solr.util.LogListener;
import org.junit.Test;
import picocli.CommandLine;

/**
 * Runs {@link SnapshotToolsTest} through picocli, using the {@code bin/solr snapshot <sub-command>}
 * group, and checks that the old {@code snapshot-*} spellings still work as hidden, deprecated
 * shims.
 */
public class SnapshotToolsPicocliTest extends SnapshotToolsTest {

  /**
   * Builds the real root command, giving each tool the test's runtime so its output is captured.
   */
  private static CommandLine root(CLITestHelper.TestingRuntime runtime) {
    CommandLine.IFactory factory =
        new CommandLine.IFactory() {
          @Override
          public <K> K create(Class<K> cls) throws Exception {
            if (ToolBase.class.isAssignableFrom(cls)) {
              try {
                return cls.getDeclaredConstructor(ToolRuntime.class).newInstance(runtime);
              } catch (NoSuchMethodException e) {
                // a shim: it has only the default constructor
              }
            }
            return CommandLine.defaultFactory().create(cls);
          }
        };
    return new CommandLine(new SolrCLI(), factory);
  }

  /** Runs a commons-cli style {@code snapshot-<x> ...} command line as {@code snapshot <x> ...}. */
  static int runAsGroup(String[] args, CLITestHelper.TestingRuntime runtime) {
    List<String> grouped =
        new ArrayList<>(List.of("snapshot", args[0].substring("snapshot-".length())));
    grouped.addAll(List.of(args).subList(1, args.length));
    return root(runtime).execute(grouped.toArray(new String[0]));
  }

  @Override
  protected int runTool(
      String[] args, CLITestHelper.TestingRuntime runtime, Class<? extends ToolBase> clazz)
      throws Exception {
    return runAsGroup(args, runtime);
  }

  @Test
  public void testOldSpellingsStillWork() throws Exception {
    CommandLine root = root(new CLITestHelper.TestingRuntime(true));
    String url = cluster.getJettySolrRunner(0).getBaseUrl().toString();

    // the notice is logged once per JVM, and this is the only test that runs the old spellings
    try (LogListener deprecation =
        LogListener.warn(DeprecationLog.LOG_PREFIX + "cli.snapshot-create")) {
      assertEquals(
          0,
          root.execute(
              "snapshot-create",
              "-c",
              COLLECTION,
              "--snapshot-name",
              "oldSpelling",
              "--solr-url",
              url));
      String notice = deprecation.pollMessage();
      assertNotNull("a deprecation notice is logged", notice);
      assertTrue(notice, notice.contains("bin/solr snapshot create"));
    }
    assertTrue(run(SnapshotListTool.class, "snapshot-list").contains("oldSpelling"));
    assertEquals(
        0,
        root.execute(
            "snapshot-delete",
            "-c",
            COLLECTION,
            "--snapshot-name",
            "oldSpelling",
            "--solr-url",
            url));
    assertFalse(run(SnapshotListTool.class, "snapshot-list").contains("oldSpelling"));
  }

  /** Goes with the shims, which are removed in Solr 11. */
  @Deprecated
  @Test
  public void testOldSpellingsAreHiddenAndDeprecated() {
    CommandLine root = root(new CLITestHelper.TestingRuntime(true));
    for (String sub : List.of("create", "delete", "describe", "export", "list")) {
      CommandLine shim = root.getSubcommands().get("snapshot-" + sub);
      assertNotNull("snapshot-" + sub, shim);
      assertTrue("snapshot-" + sub, shim.getCommandSpec().usageMessage().hidden());
      Deprecated deprecated = shim.getCommand().getClass().getAnnotation(Deprecated.class);
      assertNotNull("snapshot-" + sub, deprecated);
      assertEquals("10.2", deprecated.since());
    }
    assertFalse(root.getUsageMessage().contains("snapshot-create"));
    assertTrue(root.getUsageMessage().contains("snapshot"));
  }

  @Test
  public void testGroupListsItsSubCommands() {
    String usage =
        root(new CLITestHelper.TestingRuntime(true))
            .getSubcommands()
            .get("snapshot")
            .getUsageMessage();
    for (String sub : List.of("create", "delete", "describe", "export", "list")) {
      assertTrue(sub + " in " + usage, usage.contains(sub));
    }
  }
}
