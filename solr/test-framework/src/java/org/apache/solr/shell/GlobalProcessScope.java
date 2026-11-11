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

package org.apache.solr.shell;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Gives a test class the machine's Solr process list to itself: no other shell test class runs
 * while it does.
 *
 * <p>Needed only for a command whose result depends on processes the class did not start. In
 * practice that is {@code solr status} with no {@code --port} and no {@code --solr-url}, which asks
 * the OS for every Solr process rather than reading {@code SOLR_PID_DIR}. Under parallel forks it
 * therefore lists other classes' nodes too, so an <em>exhaustive</em> or <em>negative</em>
 * assertion over its output — "no Solr nodes are running", or a count — is a race without this.
 *
 * <p>Most tests need nothing: {@code solr stop --all} walks the per-class {@code SOLR_PID_DIR} and
 * cannot reach another class's node, and {@code status -p} / {@code stop -p} name a port. A
 * positive, partial assertion such as "my port appears in the listing" is also safe as it stands.
 *
 * <p>The cost is real — an annotated class waits for every other class to finish and blocks new
 * ones from starting — so reach for a scoped command first.
 *
 * <p><strong>It only excludes this run's own forks, not the machine.</strong> A developer with
 * their own Solr up on 8983 would still have it listed, so an annotated class is <em>skipped</em>
 * when the harness finds a Solr it did not start; killing someone's node is not the test
 * framework's business. That means an annotated class does not run everywhere, which is another
 * reason to prefer a relative assertion where one will do:
 *
 * <pre>
 *   // Needs nothing: parallel-safe, and indifferent to any other Solr on the machine.
 *   run("solr stop --all");
 *   run("solr status").assertOutputNotContains(String.valueOf(ports().solr()));
 *
 *   // Needs the annotation, and is skipped on a machine running Solr of its own.
 *   &#64;GlobalProcessScope
 *   public class SolrShellCleanupTest extends SolrShellTestCase {
 *     &#64;Test
 *     public void nothingIsLeftRunning() {
 *       run("solr status").assertOutputContains("No Solr nodes are running");
 *     }
 *   }
 * </pre>
 *
 * @see org.apache.solr.shell.env.ProcessScopeLock
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface GlobalProcessScope {}
