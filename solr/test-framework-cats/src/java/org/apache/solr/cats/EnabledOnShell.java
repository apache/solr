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

package org.apache.solr.cats;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Restricts a test, or a whole class of them, to particular shells. On any other shell it is
 * skipped as an assumption failure.
 *
 * <p>Use it where the program's launchers genuinely differ — an option only one of them accepts, a
 * per-shell configuration file, a Windows-only path quirk. The default, no annotation at all, means
 * the test is expected to pass on every shell, which is what most tests should be.
 *
 * <pre>
 *   &#64;Test
 *   &#64;EnabledOnShell("cmd")
 *   public void configFileIsSourced() { ... }
 * </pre>
 *
 * <p>On a class it skips the whole class before the program is started, so a {@code @BeforeClass}
 * that launches a process costs nothing on the wrong shell.
 *
 * @see org.apache.solr.cats.exec.Shells
 */
@Target({ElementType.TYPE, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface EnabledOnShell {

  /** The {@linkplain org.apache.solr.cats.exec.Shell#id() ids} of the shells this test is for. */
  String[] value();
}
