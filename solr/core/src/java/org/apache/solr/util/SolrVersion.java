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

import com.github.zafarkhaja.semver.ParseException;
import com.github.zafarkhaja.semver.Version;
import com.github.zafarkhaja.semver.expr.ExpressionParser;
import org.apache.solr.common.SolrException;

import java.util.Locale;

/**
 * Simple Solr version representation backed by a <a href="https://devhints.io/semver">Semantic Versioning</a> library.
 * Provides a constant for current Solr version as well as methods to parse string versions and
 * compare versions to each other.
 */
public final class SolrVersion implements Comparable<SolrVersion> {
  // Backing SemVer version
  private final Version version;

  // This static variable should be bumped for each release
  private static final String LATEST_STRING = "10.0.0";

  /**
   * This instance represents the current (latest) version of Solr.
   */
  public static final SolrVersion LATEST = SolrVersion.valueOf(LATEST_STRING);

  /**
   * Create a SolrVersion instance from string value. The string must comply to the SemVer spec
   */
  public static SolrVersion valueOf(String version) {
    return new SolrVersion(Version.valueOf(version));
  }

  /**
   * Create a SolrVersion instance from set of integer values. Must comply to the SemVer spec
   */
  public static SolrVersion forIntegers(int major, int minor, int patch) {
    return new SolrVersion(Version.forIntegers(major, minor, patch));
  }

  /**
   * Return version as plain SemVer string, e.g. "9.0.1"
   */
  @Override
  public String toString() {
    // Workaround for bug https://github.com/zafarkhaja/jsemver/issues/32
    // TODO: Needs to find a newer SemVer lib
    StringBuilder sb = new StringBuilder(String.format(Locale.ROOT, "%d.%d.%d",
        version.getMajorVersion(), version.getMinorVersion(), version.getPatchVersion()));
    if (!version.getPreReleaseVersion().isEmpty()) {
      sb.append("-").append(version.getPreReleaseVersion());
    }
    if (!version.getBuildMetadata().isEmpty()) {
      sb.append("+").append(version.getBuildMetadata());
    }
    return sb.toString();
  }

  public boolean greaterThan(SolrVersion other) {
    return version.greaterThan(other.version);
  }

  public boolean greaterThanOrEqualTo(SolrVersion other) {
    return version.greaterThanOrEqualTo(other.version);
  }

  public boolean lessThan(SolrVersion other) {
    return version.lessThan(other.version);
  }

  public boolean lessThanOrEqualTo(SolrVersion other) {
    return version.lessThanOrEqualTo(other.version);
  }

  /**
   * Returns true if this version satisfies the provided <a href="https://devhints.io/semver">SemVer Expression</a>
   * @param semVerExpression the expression to test
   * @throws InvalidSemVerExpressionException if the SemVer expression is invalid
   */
  public boolean satisfies(String semVerExpression) {
    try {
      return ExpressionParser.newInstance().parse(semVerExpression).interpret(version);
    } catch (ParseException parseException) {
      throw new InvalidSemVerExpressionException();
    }
  }

  public int getMajorVersion() {
    return version.getMajorVersion();
  }

  public int getMinorVersion() {
    return version.getMinorVersion();
  }

  public int getPatchVersion() {
    return version.getPatchVersion();
  }

  public String getPrereleaseVersion() {
    return version.getPreReleaseVersion();
  }

  // Private constructor. Should be instantiated from static factory methods
  private SolrVersion(Version version) {
    this.version = version;
  }

  @Override
  public int hashCode() {
    return version.hashCode();
  }

  @Override
  public int compareTo(SolrVersion other) {
    return version.compareTo(other.version);
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof SolrVersion)) {
      return false;
    }
    return compareTo((SolrVersion) other) == 0;
  }

  public static class InvalidSemVerExpressionException extends SolrException {
    public InvalidSemVerExpressionException() {
      super(ErrorCode.BAD_REQUEST, "Invalid SemVer expression");
    }
  }
}