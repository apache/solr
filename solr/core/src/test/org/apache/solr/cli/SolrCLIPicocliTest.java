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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import org.apache.solr.SolrTestCase;
import org.junit.Test;
import picocli.CommandLine;

/**
 * Guard-rails for the picocli command tree that apply to every command, so that porting a new tool
 * cannot silently regress them.
 */
public class SolrCLIPicocliTest extends SolrTestCase {

  private static CommandLine rootCommand() {
    return new CommandLine(new SolrCLI());
  }

  /** Every command in the tree, including the root, as "path" -> CommandLine. */
  private static List<CommandLine> allCommands(CommandLine root) {
    List<CommandLine> all = new ArrayList<>();
    all.add(root);
    collectSubcommands(root, all);
    return all;
  }

  private static void collectSubcommands(CommandLine parent, List<CommandLine> into) {
    for (CommandLine sub : parent.getSubcommands().values()) {
      into.add(sub);
      collectSubcommands(sub, into);
    }
  }

  /** The argv a user would type to reach this command, e.g. {@code ["zk", "ls"]}. */
  private static String[] pathOf(CommandLine cmd) {
    List<String> path = new ArrayList<>();
    for (CommandLine c = cmd; c != null; c = c.getParent()) {
      path.add(0, c.getCommandName());
    }
    // Drop the root "solr" element; it is not typed as an argument.
    path.remove(0);
    return path.toArray(new String[0]);
  }

  private static String[] withArg(String[] path, String arg) {
    String[] args = new String[path.length + 1];
    System.arraycopy(path, 0, args, 0, path.length);
    args[path.length] = arg;
    return args;
  }

  /**
   * Running {@code bin/solr} with no command must print usage rather than throwing picocli's
   * "Parsed command is not a Method, Runnable or Callable".
   */
  @Test
  public void testNoArgsPrintsUsageAndExitsNonZero() {
    StringWriter out = new StringWriter();
    CommandLine cmd = rootCommand();
    cmd.setOut(new PrintWriter(out));
    cmd.setErr(new PrintWriter(new StringWriter()));

    int exitCode = cmd.execute();

    assertEquals("bare invocation should exit non-zero, as the commons-cli path does", 1, exitCode);
    assertTrue("bare invocation should print usage, got: " + out, out.toString().contains("solr"));
  }

  /** {@code bin/solr --help} reaches the tool as a single empty argument via the shell script. */
  @Test
  public void testEmptyLeadingArgIsStripped() {
    assertArrayEquals(new String[0], SolrCLI.stripEmptyLeadingArg(new String[] {""}));
    assertArrayEquals(
        new String[] {"status"}, SolrCLI.stripEmptyLeadingArg(new String[] {"", "status"}));
    assertArrayEquals(
        new String[] {"status"}, SolrCLI.stripEmptyLeadingArg(new String[] {"status"}));
    assertArrayEquals(new String[0], SolrCLI.stripEmptyLeadingArg(new String[0]));
  }

  /**
   * Every command must answer both spellings of the help option. Without this, porting a tool and
   * forgetting the help mixin leaves {@code --help} reported as an unknown option.
   */
  @Test
  public void testEveryCommandSupportsHelp() {
    for (CommandLine cmd : allCommands(rootCommand())) {
      String[] path = pathOf(cmd);
      for (String helpOption : new String[] {"-h", "--help"}) {
        CommandLine root = rootCommand();
        root.setOut(new PrintWriter(new StringWriter()));
        root.setErr(new PrintWriter(new StringWriter()));
        int exitCode = root.execute(withArg(path, helpOption));
        assertEquals(
            "'bin/solr " + String.join(" ", withArg(path, helpOption)) + "' should exit 0",
            0,
            exitCode);
      }
    }
  }

  /** {@code --version} is only meaningful on the top-level command. */
  @Test
  public void testOnlyRootDeclaresVersionOption() {
    CommandLine root = rootCommand();
    List<CommandLine> subcommands = new ArrayList<>();
    collectSubcommands(root, subcommands);
    for (CommandLine cmd : subcommands) {
      for (CommandLine.Model.OptionSpec option : cmd.getCommandSpec().options()) {
        for (String name : option.names()) {
          assertFalse(
              "'bin/solr "
                  + String.join(" ", pathOf(cmd))
                  + "' must not declare "
                  + name
                  + "; --version belongs on the top-level command only",
              "--version".equals(name) || "-V".equals(name));
        }
      }
    }
  }

  /** The root command keeps the commons-cli spellings of the version option. */
  @Test
  public void testRootVersionOptionSpellings() {
    List<String> names = new ArrayList<>();
    for (CommandLine.Model.OptionSpec option : rootCommand().getCommandSpec().options()) {
      names.addAll(List.of(option.names()));
    }
    assertTrue("-v should request the version, as with commons-cli", names.contains("-v"));
    assertTrue(names.contains("--version"));
  }
}
