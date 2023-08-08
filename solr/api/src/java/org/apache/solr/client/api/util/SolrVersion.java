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
package org.apache.solr.client.api.util;

import java.util.Locale;
import org.semver4j.Semver;

/**
 * Simple Solr version representation backed by a <a href="https://devhints.io/semver">Semantic
 * Versioning</a> library. Provides a constant for current Solr version as well as methods to parse
 * string versions and compare versions to each other.
 */
public final class SolrVersion implements Comparable<SolrVersion> {
  // Backing SemVer version
  private final Semver version;

  // This static variable should be bumped for each release
  public static final String LATEST_STRING = "9.4.0";

  /** This instance represents the current (latest) version of Solr. */
  public static final SolrVersion LATEST = SolrVersion.valueOf(LATEST_STRING);

  /** Create a SolrVersion instance from string value. The string must comply to the SemVer spec */
  public static SolrVersion valueOf(String version) {
    return new SolrVersion(new Semver(version));
  }

  /** Create a SolrVersion instance from set of integer values. Must comply to the SemVer spec */
  public static SolrVersion forIntegers(int major, int minor, int patch) {
    return new SolrVersion(new Semver(String.format(Locale.ROOT, "%d.%d.%d", major, minor, patch)));
  }

  /**
   * Compares two versions v1 and v2. Returns negative if v1 isLessThan v2, positive if v1
   * isGreaterThan v2 and 0 if equal.
   */
  public static int compareVersions(String v1, String v2) {
    return new Semver(v1).compareTo(new Semver(v2));
  }

  /** Return version as plain SemVer string, e.g. "9.0.1" */
  @Override
  public String toString() {
    return version.toString();
  }

  public boolean greaterThan(SolrVersion other) {
    return version.isGreaterThan(other.version);
  }

  public boolean greaterThanOrEqualTo(SolrVersion other) {
    return version.isGreaterThanOrEqualTo(other.version);
  }

  public boolean lessThan(SolrVersion other) {
    return version.isLowerThan(other.version);
  }

  public boolean lessThanOrEqualTo(SolrVersion other) {
    return version.isLowerThanOrEqualTo(other.version);
  }

  /**
   * Returns true if this version satisfies the provided <a href="https://devhints.io/semver">SemVer
   * Expression</a>
   *
   * @param semVerExpression the expression to test
   */
  public boolean satisfies(String semVerExpression) {
    return version.satisfies(semVerExpression);
  }

  public int getMajorVersion() {
    return version.getMajor();
  }

  public int getMinorVersion() {
    return version.getMinor();
  }

  public int getPatchVersion() {
    return version.getPatch();
  }

  public String getPrereleaseVersion() {
    return String.join(".", version.getPreRelease());
  }

  // Private constructor. Should be instantiated from static factory methods
  private SolrVersion(Semver version) {
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
}
