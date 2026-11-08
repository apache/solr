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
import picocli.CommandLine;

/** Parsing-level tests of {@link PackageTargetOptions}, shared by package deploy and undeploy. */
public class PackageTargetOptionsTest extends SolrTestCase {

  @CommandLine.Command(name = "probe")
  static class Probe {
    @CommandLine.Mixin PackageTargetOptions targetOptions;
  }

  private static PackageTargetOptions parse(String... args) {
    Probe probe = new Probe();
    new CommandLine(probe).parseArgs(args);
    return probe.targetOptions;
  }

  @Test
  public void testClusterTarget() {
    PackageTargetOptions target = parse("--cluster");
    assertTrue(target.cluster);
    assertNull(target.collections);
    assertNull(target.missingTargetMessage("deploy"));
  }

  @Test
  public void testCollectionsTarget() {
    PackageTargetOptions target = parse("--collections", "c1,c2");
    assertFalse(target.cluster);
    assertEquals("c1,c2", target.collections);
    assertNull(target.missingTargetMessage("deploy"));
  }

  @Test
  public void testBothTargetsAreAccepted() {
    PackageTargetOptions target = parse("--cluster", "--collections", "c1");
    assertTrue(target.cluster);
    assertEquals("c1", target.collections);
    assertNull(target.missingTargetMessage("deploy"));
  }

  @Test
  public void testNoTargetReportsTheVerb() {
    PackageTargetOptions target = parse();
    String message = target.missingTargetMessage("undeploy");
    assertNotNull(message);
    assertTrue(message, message.contains("--cluster"));
    assertTrue(message, message.contains("--collections"));
    assertTrue(message, message.contains("undeploy"));
  }
}
