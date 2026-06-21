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

import org.apache.solr.SolrTestCase;
import org.junit.Test;

public class SolrCLITest extends SolrTestCase {

  @Test
  public void testUptime() {
    assertEquals("?", SolrCLI.uptime(0));
    assertEquals("0 days, 0 hours, 0 minutes, 0 seconds", SolrCLI.uptime(1));

    assertEquals(
        "Should have rounded down", "0 days, 0 hours, 0 minutes, 0 seconds", SolrCLI.uptime(499));
    assertEquals(
        "Should have rounded up", "0 days, 0 hours, 0 minutes, 1 seconds", SolrCLI.uptime(501));

    // Overflow
    assertEquals("24 days, 20 hours, 31 minutes, 24 seconds", SolrCLI.uptime(Integer.MAX_VALUE));
    assertEquals(
        "106751991167 days, 7 hours, 12 minutes, 56 seconds", SolrCLI.uptime(Long.MAX_VALUE));
  }

  @Test
  public void testFirstLineOnlyHelpFactoryTruncatesMultiLineDescription() {
    picocli.CommandLine cmd = new picocli.CommandLine(new MultiLineDescriptionCommand());

    String baseline = cmd.getUsageMessage();
    assertTrue("baseline should include first line", baseline.contains("First line."));
    assertTrue("baseline should include second line", baseline.contains("Second line."));

    SolrCLI.installFirstLineOnlyHelpFactory(cmd);
    String truncated = cmd.getUsageMessage();
    assertTrue("truncated should include first line", truncated.contains("First line."));
    assertFalse("truncated should NOT include second line", truncated.contains("Second line."));
    assertFalse("truncated should NOT include third line", truncated.contains("Third line."));
  }

  @picocli.CommandLine.Command(name = "test")
  private static class MultiLineDescriptionCommand {
    @picocli.CommandLine.Option(
        names = "--multi",
        description = {"First line.", "Second line.", "Third line."})
    String multi;
  }
}
