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
package org.apache.solr.bench.generators;

import static org.apache.solr.bench.generators.SourceDSL.checkArguments;

import java.util.Date;
import org.apache.solr.bench.SolrGenerate;

/**
 * A Class for creating Date Sources that will produce Dates based on the number of milliseconds
 * since epoch
 */
public class DatesDSL {

  public SolrGen<Date> all() {
    return Dates.withMilliSecondsBetween(0, Long.MAX_VALUE);
  }

  /**
   * Generates Dates inclusively bounded between January 1, 1970, 00:00:00 GMT and new
   * Date(milliSecondsFromEpoch). The Source restricts Date generation, so that no Dates before 1970
   * can be created. The Source is weighted, so it is likely to produce new
   * Date(millisecondsFromEpoch) one or more times.
   *
   * @param millisecondsFromEpoch the number of milliseconds from the epoch such that Dates are
   *     generated within this interval.
   * @return a Source of type Date
   */
  public SolrGen<Date> withMilliseconds(long millisecondsFromEpoch) {
    lowerBoundGEQZero(millisecondsFromEpoch);
    return Dates.withMilliSeconds(millisecondsFromEpoch);
  }

  /**
   * Generates Dates inclusively bounded between new Date(millisecondsFromEpochStartInclusive) and
   * new Date(millisecondsFromEpochEndInclusive).
   *
   * @param millisecondsFromEpochStartInclusive the number of milliseconds from epoch for the
   *     desired older Date
   * @param millisecondsFromEpochEndInclusive the number of milliseconds from epoch for the desired
   *     more recent Date
   * @return a source of Dates
   */
  public SolrGen<Date> withMillisecondsBetween(
      long millisecondsFromEpochStartInclusive, long millisecondsFromEpochEndInclusive) {
    lowerBoundGEQZero(millisecondsFromEpochStartInclusive);
    maxGEQMin(millisecondsFromEpochStartInclusive, millisecondsFromEpochEndInclusive);
    return Dates.withMilliSecondsBetween(
        millisecondsFromEpochStartInclusive, millisecondsFromEpochEndInclusive);
  }

  private void lowerBoundGEQZero(long milliSecondsFromEpoch) {
    checkArguments(
        milliSecondsFromEpoch >= 0,
        "A negative long (%s) is not an accepted number of milliseconds",
        milliSecondsFromEpoch);
  }

  private void maxGEQMin(long startInclusive, long endInclusive) {
    checkArguments(
        startInclusive <= endInclusive,
        "Cannot have the maximum long (%s) smaller than the minimum long value (%s)",
        endInclusive,
        startInclusive);
  }

  static class Dates {

    private Dates() {}

    static SolrGen<Date> withMilliSeconds(long milliSecondsFromEpoch) {
      return withMilliSecondsBetween(0, milliSecondsFromEpoch);
    }

    static SolrGen<Date> withMilliSecondsBetween(
        long milliSecondsFromEpochStartInclusive, long milliSecondsFromEpochEndInclusive) {
      return SolrGenerate.longRange(
              milliSecondsFromEpochStartInclusive, milliSecondsFromEpochEndInclusive)
          .map(Date::new, Date.class);
    }
  }
}
