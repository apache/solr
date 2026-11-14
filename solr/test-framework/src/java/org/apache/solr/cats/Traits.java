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

import static org.junit.Assume.assumeTrue;

import java.util.List;
import java.util.Map;
import org.apache.solr.cats.exec.ProgramUnderTest;

/**
 * The two things a test does with the {@linkplain ProgramUnderTest#traits named traits of a run}:
 * refuse to run unless a trait has one of the values the test was written for, and pick the wording
 * that trait's value implies.
 *
 * <p>Which shell a run uses is the one trait the harness itself knows ({@link EnabledOnShell});
 * every other is a name/value pair the program reports off its environment, so the harness ships no
 * annotation for one. A project with a trait to gate on writes its own annotation and a short rule
 * over {@link #assume} — which is where a typed, well-named annotation belongs: with whoever knows
 * the trait.
 */
public final class Traits {

  private Traits() {}

  /**
   * Skips as an assumption failure unless {@code name} has one of {@code allowed} in this run.
   *
   * @param traits the run's traits
   * @param name the trait to check
   * @param allowed the values the caller was written for
   */
  public static void assume(Map<String, String> traits, String name, String... allowed) {
    String actual = traits.get(name);
    List<String> expected = List.of(allowed);
    assumeTrue(
        "Only runs with " + name + " in " + expected + "; this run has " + name + "=" + actual,
        expected.contains(actual));
  }

  /**
   * The wording {@code name}'s value calls for.
   *
   * <p>For a test that holds for every value of a trait bar a word or two of the program's output,
   * which keeps one test covering them all.
   *
   * @param traits the run's traits
   * @param name the trait the wording depends on
   * @param byValue the wording for each value the trait can take
   * @throws IllegalArgumentException when this run's value is not among them
   */
  public static String wordingFor(
      Map<String, String> traits, String name, Map<String, String> byValue) {
    String value = traits.get(name);
    String wording = byValue.get(value);
    if (wording == null) {
      throw new IllegalArgumentException(
          "No wording given for " + name + "=" + value + "; have " + byValue.keySet());
    }
    return wording;
  }
}
