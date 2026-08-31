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

package org.apache.solr.util;

import static org.hamcrest.core.StringContains.containsString;

import java.lang.invoke.MethodHandles;
import java.util.regex.Pattern;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.util.SuppressForbidden;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressForbidden(
    reason = "We need to use log4J2 classes directly to check that the ErrorLogMuter is working")
public class TestErrorLogMuter extends SolrTestCaseJ4 {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @LogLevel("=WARN")
  public void testErrorMutingRegex() {

    try (LogListener rootWarnCheck = LogListener.warn("");
        LogListener rootErrorCheck = LogListener.error("")) {

      try (ErrorLogMuter x = ErrorLogMuter.regex("eRrOr\\s+Log")) {
        assertEquals(0, x.getCount());

        log.error("This is an {} Log msg that x should be muted", "eRrOr");
        assertEquals(1, x.getCount());

        log.error("This is an {} Log msg that x should not mute", "err");
        log.warn("This is an warn message, mentioning 'eRrOr Log', that should also not be muted");
        assertEquals(1, x.getCount());
        // the root logger better have gotten both of those...
        assertThat(rootErrorCheck.pollMessage(), containsString("should not mute"));
        assertThat(rootWarnCheck.pollMessage(), containsString("also not be muted"));

        log.error(
            "This {} because of the {} msg",
            "error",
            "throwable",
            new Exception("outer", new Exception("inner eRrOr Log throwable")));
        assertEquals(2, x.getCount());
      }

      // the root logger must have only gotten the non-muted messages...
      assertEquals(1, rootErrorCheck.getCount());
      assertEquals(1, rootWarnCheck.getCount());
    }
  }

  @LogLevel("=WARN")
  public void testMultipleMuters() {

    try (LogListener rootWarnCheck = LogListener.warn("").substring("xxx");
        LogListener rootErrorCheck = LogListener.error("")) {

      // sanity check that muters "mute" in the order used...
      // (If this fails, then it means log4j has changed the precedence order it uses when addFilter
      // is called,
      // if that happens, we'll need to change our impl to check if an impl of some special
      // "container" Filter subclass we create.
      // is in the list of ROOT filters -- if not add one, and then "add" the specific muting filter
      // to our "container" Filter)
      try (ErrorLogMuter x = ErrorLogMuter.substring("xxx");
          ErrorLogMuter y = ErrorLogMuter.regex(Pattern.compile("YYY", Pattern.CASE_INSENSITIVE));
          ErrorLogMuter z = ErrorLogMuter.regex("(xxx|yyy)")) {

        log.error("xx{}  ", "x");
        log.error("    yyy");
        log.error("xxx yyy");

        // a warning shouldn't be muted...
        log.warn("xxx  yyy");
        assertEquals("xxx  yyy", rootWarnCheck.pollMessage());

        log.error("abc", new Exception("yyy"));

        assertEquals(2, x.getCount()); // x is first, so it swallows up the "x + y" message
        assertEquals(2, y.getCount()); // doesn't get anything x already got
        assertEquals(0, z.getCount()); // doesn't get anything x already got
      }

      // the root logger must have only gotten the non-muted messages...
      assertEquals(1, rootWarnCheck.getCount());
      assertEquals(0, rootErrorCheck.getCount());
    }
  }
}
