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

package org.apache.solr.shell.session;

import java.time.Duration;
import java.util.List;
import org.apache.solr.cats.exec.CommandResult;
import org.apache.solr.cats.session.CatsCliHelpers;
import org.apache.solr.cats.session.CliSession;

/**
 * The Solr-aware conveniences a shell test reaches for, ported from {@code bats_helper.bash}:
 * collections, config sets, a shutdown of everything, and the Solr log.
 *
 * <p>The HTTP families and the file tail come from {@link CatsCliHelpers}; what is here is only
 * what names something Solr has.
 */
public final class SolrCliHelpers extends CatsCliHelpers {

  private final CliSession cli;

  public SolrCliHelpers(CliSession cli) {
    super(cli.harnessEnv(), cli.shell(), cli.transcript(), cli.varsForAssertions());
    this.cli = cli;
  }

  /** Whether the named collection is in ZooKeeper. Port of {@code collection_exists}. */
  public boolean collectionExists(String collection) {
    return zkChildren("/collections").contains(collection);
  }

  /** Whether the named config set is in ZooKeeper. Port of {@code config_exists}. */
  public boolean configExists(String configName) {
    return zkChildren("/configs").contains(configName);
  }

  /**
   * The child node names under a ZooKeeper path.
   *
   * <p>Reads stdout alone, then keeps only lines that could be a node name. Both filters earn their
   * place: addressing ZooKeeper directly with {@code -z} makes SolrZkClient log a WARN about
   * default ACLs, and the BATS helper this replaces fed exactly that line back into {@code solr
   * delete}. BATS never noticed, because it discarded the delete's output.
   */
  private List<String> zkChildren(String zkPath) {
    CommandResult result = cli.runSeparated("solr zk ls " + zkPath + " -z localhost:${ZK_PORT}");
    if (result.failed()) {
      return List.of();
    }
    return result
        .stdout()
        .lines()
        .map(String::trim)
        // A ZooKeeper node name carries no whitespace, while every log line does.
        .filter(line -> !line.isEmpty() && line.chars().noneMatch(Character::isWhitespace))
        .toList();
  }

  /** Port of {@code delete_all_collections}. */
  public void deleteAllCollections() {
    for (String collection : zkChildren("/collections")) {
      cli.run("solr delete -c " + collection + " --delete-config");
    }
  }

  /**
   * Port of {@code shutdown_all}: {@code solr stop --all}, ignoring the outcome.
   *
   * <p>Scoped to this class, because {@code bin/solr} implements {@code --all} by walking {@code
   * SOLR_PID_DIR} and the harness gives every class its own.
   */
  public void shutdownAll() {
    try {
      cli.run(Duration.ofMinutes(2), "solr stop --all");
    } catch (RuntimeException | AssertionError e) {
      // Cleanup helper: the caller is usually already handling a failure.
    }
  }

  /** Prints the tail of {@code solr.log} to stderr, for debugging a failing test. */
  public void dumpSolrLog() {
    tailFile(cli.harnessEnv().logDir().resolve("solr.log"), 200);
  }
}
