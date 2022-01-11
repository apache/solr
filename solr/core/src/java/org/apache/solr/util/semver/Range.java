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

import java.util.Objects;

public class Range {
  protected final Semver version;
  protected final RangeOperator op;

  public Range(Semver version, RangeOperator op) {
    this.version = version;
    this.op = op;
  }

  public Range(String version, RangeOperator op) {
    this(new Semver(version, Semver.SemverType.LOOSE), op);
  }

  public boolean isSatisfiedBy(String version) {
    return this.isSatisfiedBy(new Semver(version, this.version.getType()));
  }

  public boolean isSatisfiedBy(Semver version) {
    switch (this.op) {
      case EQ:
        return version.isEquivalentTo(this.version);
      case LT:
        return version.isLowerThan(this.version);
      case LTE:
        return version.isLowerThan(this.version) || version.isEquivalentTo(this.version);
      case GT:
        return version.isGreaterThan(this.version);
      case GTE:
        return version.isGreaterThan(this.version) || version.isEquivalentTo(this.version);
    }

    throw new RuntimeException("Code error. Unknown RangeOperator: " + this.op); // Should never happen
  }

  @Override public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof Range)) return false;
    Range range = (Range) o;
    return Objects.equals(version, range.version) &&
        op == range.op;
  }

  @Override public int hashCode() {
    return Objects.hash(version, op);
  }

  @Override public String toString() {
    return this.op.asString() + this.version;
  }

  public enum RangeOperator {
    /**
     * The version and the requirement are equivalent
     */
    EQ("="),

    /**
     * The version is lower than the requirent
     */
    LT("<"),

    /**
     * The version is lower than or equivalent to the requirement
     */
    LTE("<="),

    /**
     * The version is greater than the requirement
     */
    GT(">"),

    /**
     * The version is greater than or equivalent to the requirement
     */
    GTE(">=");

    private final String s;

    RangeOperator(String s) {
      this.s = s;
    }

    public String asString() {
      return s;
    }
  }
}
