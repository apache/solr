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

// Forked and adapted from https://github.com/vdurmont/semver4j - MIT license
// Copyright (c) 2015-present Vincent DURMONT vdurmont@gmail.com

package org.apache.solr.util.semver;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;

import org.apache.solr.util.semver.Semver.SemverType;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;


@RunWith(Parameterized.class)
public class NpmSemverTest {

  private String version;
  private String rangeExpression;
  private boolean expected;

  public NpmSemverTest(String version, String rangeExpression, boolean expected) {
    this.version = version;
    this.rangeExpression = rangeExpression;
    this.expected = expected;
  }

  @Parameters
  public static Iterable<Object[]> getParameters() {
    return Arrays.asList(new Object[][] {
        // Fully-qualified versions:
        { "1.2.3", "1.2.3", true, },
        { "1.2.4", "1.2.3", false, },

        // Minor versions:
        { "1.2.3", "1.2", true, },
        { "1.2.4", "1.3", false, },

        // Major versions:
        { "1.2.3", "1", true, },
        { "1.2.4", "2", false, },

        // Hyphen ranges:
        { "1.2.4-beta+exp.sha.5114f85", "1.2.3 - 2.3.4", false, },
        { "1.2.4", "1.2.3 - 2.3.4", true, },
        { "1.2.3", "1.2.3 - 2.3.4", true, },
        { "2.3.4", "1.2.3 - 2.3.4", true, },
        { "2.3.0-alpha", "1.2.3 - 2.3.0-beta", true, },
        { "2.3.4", "1.2.3 - 2.3", true, },
        { "2.3.4", "1.2.3 - 2", true, },
        { "4.4", "3.X - 4.X", true, },
        { "1.0.0", "1.2.3 - 2.3.4", false, },
        { "3.0.0", "1.2.3 - 2.3.4", false, },
        { "2.4.3", "1.2.3 - 2.3", false, },
        { "2.3.0-rc1", "1.2.3 - 2.3.0-beta", false, },
        { "3.0.0", "1.2.3 - 2", false, },

        // Wildcard ranges:
        { "3.1.5", "", true, },
        { "3.1.5", "*", true, },
        { "0.0.0", "*", true, },
        { "1.0.0-beta", "*", true, },
        { "3.1.5-beta", "3.1.x", false, },
        { "3.1.5-beta+exp.sha.5114f85", "3.1.x", false, },
        { "3.1.5+exp.sha.5114f85", "3.1.x", true, },
        { "3.1.5", "3.1.x", true, },
        { "3.1.5", "3.1.X", true, },
        { "3.1.5", "3.x", true, },
        { "3.1.5", "3.*", true, },
        { "3.1.5", "3.1", true, },
        { "3.1.5", "3", true, },
        { "3.2.5", "3.1.x", false, },
        { "3.0.5", "3.1.x", false, },
        { "4.0.0", "3.x", false, },
        { "2.0.0", "3.x", false, },
        { "3.2.5", "3.1", false, },
        { "3.0.5", "3.1", false, },
        { "4.0.0", "3", false, },
        { "2.0.0", "3", false, },

        // Tilde ranges:
        { "1.2.4-beta", "~1.2.3", false, },
        { "1.2.4-beta+exp.sha.5114f85", "~1.2.3", false, },
        { "1.2.3", "~1.2.3", true, },
        { "1.2.7", "~1.2.3", true, },
        { "1.2.2", "~1.2", true, },
        { "1.2.0", "~1.2", true, },
        { "1.3.0", "~1", true, },
        { "1.0.0", "~1", true, },
        { "1.2.3", "~1.2.3-beta.2", true, },
        { "1.2.3-beta.4", "~1.2.3-beta.2", true, },
        { "1.2.4", "~1.2.3-beta.2", true, },
        { "1.3.0", "~1.2.3", false, },
        { "1.2.2", "~1.2.3", false, },
        { "1.1.0", "~1.2", false, },
        { "1.3.0", "~1.2", false, },
        { "2.0.0", "~1", false, },
        { "0.0.0", "~1", false, },
        { "1.2.3-beta.1", "~1.2.3-beta.2", false, },

        // Caret ranges:
        { "1.2.3", "^1.2.3", true, },
        { "1.2.4", "^1.2.3", true, },
        { "1.3.0", "^1.2.3", true, },
        { "0.2.3", "^0.2.3", true, },
        { "0.2.4", "^0.2.3", true, },
        { "0.0.3", "^0.0.3", true, },
        { "0.0.3+exp.sha.5114f85", "^0.0.3", true, },
        { "0.0.3", "^0.0.3-beta", true, },
        { "0.0.3-pr.2", "^0.0.3-beta", true, },
        { "1.2.2", "^1.2.3", false, },
        { "2.0.0", "^1.2.3", false, },
        { "0.2.2", "^0.2.3", false, },
        { "0.3.0", "^0.2.3", false, },
        { "0.0.4", "^0.0.3", false, },
        { "0.0.3-alpha", "^0.0.3-beta", false, },
        { "0.0.4", "^0.0.3-beta", false, },

        // Comparators:
        { "2.0.0", "=2.0.0", true, },
        { "2.0.0", "=2.0", true, },
        { "2.0.1", "=2.0", true, },
        { "2.0.0", "=2", true, },
        { "2.0.1", "=2", true, },
        { "2.0.1", "=2.0.0", false, },
        { "1.9.9", "=2.0.0", false, },
        { "1.9.9", "=2.0", false, },
        { "1.9.9", "=2", false, },

        { "2.0.1", ">2.0.0", true, },
        { "3.0.0", ">2.0.0", true, },
        { "3.0.0", ">2.0", true, },
        { "3.0.0", ">2", true, },
        { "2.0.0", ">2.0.0", false, },
        { "1.9.9", ">2.0.0", false, },
        { "2.0.0", ">2.0", false, },
        { "1.9.9", ">2.0", false, },
        { "2.0.1", ">2", false, },
        { "2.0.0", ">2", false, },
        { "1.9.9", ">2", false, },

        { "1.9.9", "<2.0.0", true, },
        { "1.9.9", "<2.0", true, },
        { "1.9.9", "<2", true, },
        { "2.0.0", "<2.0.0", false, },
        { "2.0.1", "<2.0.0", false, },
        { "2.0.1", "<2.1", true, },
        { "3.0.0", "<2.0.0", false, },
        { "2.0.0", "<2.0", false, },
        { "2.0.1", "<2.0", false, },
        { "3.0.0", "<2.0", false, },
        { "2.0.0", "<2", false, },
        { "2.0.1", "<2", false, },
        { "3.0.0", "<2", false, },

        { "2.0.0", ">=2.0.0", true, },
        { "2.0.1", ">=2.0.0", true, },
        { "3.0.0", ">=2.0.0", true, },
        { "2.0.0", ">=2.0", true, },
        { "3.0.0", ">=2.0", true, },
        { "2.0.0", ">=2", true, },
        { "2.0.1", ">=2", true, },
        { "3.0.0", ">=2", true, },
        { "1.9.9", ">=2.0.0", false, },
        { "1.9.9", ">=2.0", false, },
        { "1.9.9", ">=2", false, },

        { "1.9.9", "<=2.0.0", true, },
        { "2.0.0", "<=2.0.0", true, },
        { "1.9.9", "<=2.0", true, },
        { "2.0.0", "<=2.0", true, },
        { "2.0.1", "<=2.0", true, },
        { "1.9.9", "<=2", true, },
        { "2.0.0", "<=2", true, },
        { "2.0.1", "<=2", true, },
        { "2.0.1", "<=2.0.0", false, },
        { "3.0.0", "<=2.0.0", false, },
        { "3.0.0", "<=2.0", false, },
        { "3.0.0", "<=2", false, },

        // AND ranges:
        { "2.0.1", ">2.0.0 <3.0.0", true, },
        { "2.0.1", ">2.0 <3.0", false, },

        { "1.2.0", "1.2 <1.2.8", true, },
        { "1.2.7", "1.2 <1.2.8", true, },
        { "1.1.9", "1.2 <1.2.8", false, },
        { "1.2.9", "1.2 <1.2.8", false, },

        // OR ranges:
        { "1.2.3", "1.2.3 || 1.2.4", true, },
        { "1.2.4", "1.2.3 || 1.2.4", true, },
        { "1.2.5", "1.2.3 || 1.2.4", false, },

        // Complex ranges:
        { "1.2.2", ">1.2.1 <1.2.8 || >2.0.0", true, },
        { "1.2.7", ">1.2.1 <1.2.8 || >2.0.0", true, },
        { "2.0.1", ">1.2.1 <1.2.8 || >2.0.0", true, },
        { "1.2.1", ">1.2.1 <1.2.8 || >2.0.0", false, },
        { "2.0.0", ">1.2.1 <1.2.8 || >2.0.0", false, },

        { "1.2.2", ">1.2.1 <1.2.8 || >2.0.0 <3.0.0", true, },
        { "1.2.7", ">1.2.1 <1.2.8 || >2.0.0 <3.0.0", true, },
        { "2.0.1", ">1.2.1 <1.2.8 || >2.0.0 <3.0.0", true, },
        { "2.5.0", ">1.2.1 <1.2.8 || >2.0.0 <3.0.0", true, },
        { "1.2.1", ">1.2.1 <1.2.8 || >2.0.0 <3.0.0", false, },
        { "1.2.8", ">1.2.1 <1.2.8 || >2.0.0 <3.0.0", false, },
        { "2.0.0", ">1.2.1 <1.2.8 || >2.0.0 <3.0.0", false, },
        { "3.0.0", ">1.2.1 <1.2.8 || >2.0.0 <3.0.0", false, },

        { "1.2.2", "1.2.2 - 1.2.7 || 2.0.1 - 2.9.9", true, },
        { "1.2.7", "1.2.2 - 1.2.7 || 2.0.1 - 2.9.9", true, },
        { "2.0.1", "1.2.2 - 1.2.7 || 2.0.1 - 2.9.9", true, },
        { "2.5.0", "1.2.2 - 1.2.7 || 2.0.1 - 2.9.9", true, },
        { "1.2.1", "1.2.2 - 1.2.7 || 2.0.1 - 2.9.9", false, },
        { "1.2.8", "1.2.2 - 1.2.7 || 2.0.1 - 2.9.9", false, },
        { "2.0.0", "1.2.2 - 1.2.7 || 2.0.1 - 2.9.9", false, },
        { "3.0.0", "1.2.2 - 1.2.7 || 2.0.1 - 2.9.9", false, },

        { "1.2.0", "1.2 <1.2.8 || >2.0.0", true, },
        { "1.2.7", "1.2 <1.2.8 || >2.0.0", true, },
        { "1.2.7", "1.2 <1.2.8 || >2.0.0", true, },
        { "2.0.1", "1.2 <1.2.8 || >2.0.0", true, },
        { "1.1.0", "1.2 <1.2.8 || >2.0.0", false, },
        { "1.2.9", "1.2 <1.2.8 || >2.0.0", false, },
        { "2.0.0", "1.2 <1.2.8 || >2.0.0", false, },
    });
  }

  @Test
  public void test() {
    assertEquals(this.version + " , " + this.rangeExpression ,this.expected, new Semver(this.version, SemverType.NPM).satisfies(this.rangeExpression));
  }

}
