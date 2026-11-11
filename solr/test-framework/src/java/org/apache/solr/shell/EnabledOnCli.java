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
 * Restricts a {@link SolrShellTestCase} test, or a whole class of them, to one command-line
 * implementation. On the other it is skipped as an assumption failure.
 *
 * <p>For the handful of tests that cannot hold for both, usually because they assert on usage or
 * error wording the two phrase differently. Where only a word or two differs, prefer {@code
 * CommandResult.assertContainsForCli(commonsCliText, picocliText)}, which keeps one test covering
 * both.
 *
 * <pre>
 *   &#64;Test
 *   &#64;EnabledOnCli(Cli.PICOCLI)
 *   public void picocliRejectsMutuallyExclusiveOptions() { ... }
 * </pre>
 *
 * <p>On a class it skips before any Solr starts, so a {@code @BeforeClass} that starts a node costs
 * nothing on the other implementation.
 *
 * @see Cli
 * @see EnabledOnShell
 */
@Target({ElementType.TYPE, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface EnabledOnCli {

  /** The implementations this test is meant for. */
  Cli[] value();
}
