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

/**
 * Semver is a tool that provides useful methods to manipulate versions that follow the "semantic versioning" specification
 * (see http://semver.org)
 */
public class Semver implements Comparable<Semver> {
  private final String originalValue;
  private final String value;
  private final Integer major;
  private final Integer minor;
  private final Integer patch;
  private final String[] suffixTokens;
  private final String build;
  private final SemverType type;

  public Semver(String value) {
    this(value, SemverType.STRICT);
  }

  public Semver(String value, SemverType type) {
    this.originalValue = value;
    this.type = type;
    value = value.trim();
    if (type == SemverType.NPM && (value.startsWith("v") || value.startsWith("V"))) {
      value = value.substring(1).trim();
    }
    this.value = value;
    String[] tokens;

    if (hasPreRelease(value)) {
      tokens = value.split("-", 2);
    } else {
      tokens = new String[] { value };
    }

    String build = null;
    Integer minor = null;
    Integer patch = null;
    try {
      String[] mainTokens;
      if (tokens.length == 1) {
        // The build version may be in the main tokens
        if (tokens[0].endsWith("+")) {
          throw new SemverException("The build cannot be empty.");
        }
        String[] tmp = tokens[0].split("\\+");
        mainTokens = tmp[0].split("\\.");
        if (tmp.length == 2) {
          build = tmp[1];
        }
      } else {
        mainTokens = tokens[0].split("\\.");
      }

      try {
        this.major = Integer.valueOf(mainTokens[0]);
      } catch (NumberFormatException e) {
        throw new SemverException("Invalid version (no major version): " + value);
      } catch (IndexOutOfBoundsException e) {
        throw new SemverException("Invalid version (no major version): " + value);
      }

      try {
        minor = Integer.valueOf(mainTokens[1]);
      } catch (IndexOutOfBoundsException e) {
        if (type == SemverType.STRICT) {
          throw new SemverException("Invalid version (no minor version): " + value);
        }
      } catch (NumberFormatException e) {
        if (type != SemverType.NPM || (!"x".equalsIgnoreCase(mainTokens[1]) && !"*".equals(mainTokens[1]))) {
          throw new SemverException("Invalid version (no minor version): " + value);
        }
      }
      try {
        patch = Integer.valueOf(mainTokens[2]);
      } catch (IndexOutOfBoundsException e) {
        if (type == SemverType.STRICT) {
          throw new SemverException("Invalid version (no patch version): " + value);
        }
      } catch (NumberFormatException e) {
        if (type != SemverType.NPM || (!"x".equalsIgnoreCase(mainTokens[2]) && !"*".equals(mainTokens[2]))) {
          throw new SemverException("Invalid version (no patch version): " + value);
        }
      }
    } catch (NumberFormatException e) {
      throw new SemverException("The version is invalid: " + value);
    } catch (IndexOutOfBoundsException e) {
      throw new SemverException("The version is invalid: " + value);
    }
    this.minor = minor;
    this.patch = patch;

    String[] suffix = new String[0];
    try {
      // The build version may be in the suffix tokens
      if (tokens[1].endsWith("+")) {
        throw new SemverException("The build cannot be empty.");
      }
      String[] tmp = tokens[1].split("\\+");
      if (tmp.length == 2) {
        suffix = tmp[0].split("\\.");
        build = tmp[1];
      } else {
        suffix = tokens[1].split("\\.");
      }
    } catch (IndexOutOfBoundsException ignored) {
    }
    this.suffixTokens = suffix;

    this.build = build;

    this.validate(type);
  }

  private void validate(SemverType type) {
    if (this.minor == null && type == SemverType.STRICT) {
      throw new SemverException("Invalid version (no minor version): " + value);
    }
    if (this.patch == null && type == SemverType.STRICT) {
      throw new SemverException("Invalid version (no patch version): " + value);
    }
  }

  private boolean hasPreRelease(String version) {

    int firstIndexOfPlus = value.indexOf("+");
    int firstIndexOfHyphen = value.indexOf("-");

    if (firstIndexOfHyphen == -1) {
      return false;
    }

    return firstIndexOfPlus == -1 || firstIndexOfHyphen < firstIndexOfPlus;
  }

  /**
   * Check if the version satisfies a requirement
   *
   * @param requirement the requirement
   *
   * @return true if the version satisfies the requirement
   */
  public boolean satisfies(Requirement requirement) {
    return requirement.isSatisfiedBy(this);
  }

  /**
   * Check if the version satisfies a requirement
   *
   * @param requirement the requirement
   *
   * @return true if the version satisfies the requirement
   */
  public boolean satisfies(String requirement) {
    Requirement req;
    switch (this.type) {
      case STRICT:
        req = Requirement.buildStrict(requirement);
        break;
      case LOOSE:
        req = Requirement.buildLoose(requirement);
        break;
      case NPM:
        req = Requirement.buildNPM(requirement);
        break;
      case COCOAPODS:
        req = Requirement.buildCocoapods(requirement);
        break;
      case IVY:
        req = Requirement.buildIvy(requirement);
        break;
      default:
        throw new SemverException("Invalid requirement type: " + type);
    }
    return this.satisfies(req);
  }

  /**
   * @see #isGreaterThan(Semver)
   *
   * @param version the version to compare
   *
   * @return true if the current version is greater than the provided version
   */
  public boolean isGreaterThan(String version) {
    return this.isGreaterThan(new Semver(version, this.getType()));
  }

  /**
   * Checks if the version is greater than another version
   *
   * @param version the version to compare
   *
   * @return true if the current version is greater than the provided version
   */
  public boolean isGreaterThan(Semver version) {
    // Compare the main part
    if (this.getMajor() > version.getMajor()) return true;
    else if (this.getMajor() < version.getMajor()) return false;

    if (this.type == SemverType.NPM && version.getMinor() == null) return false;

    int otherMinor = version.getMinor() != null ? version.getMinor() : 0;
    if (this.getMinor() != null && this.getMinor() > otherMinor) return true;
    else if (this.getMinor() != null && this.getMinor() < otherMinor) return false;

    if (this.type == SemverType.NPM && version.getPatch() == null) return false;

    int otherPatch = version.getPatch() != null ? version.getPatch() : 0;
    if (this.getPatch() != null && this.getPatch() > otherPatch) return true;
    else if (this.getPatch() != null && this.getPatch() < otherPatch) return false;

    // Let's take a look at the suffix
    String[] tokens1 = this.getSuffixTokens();
    String[] tokens2 = version.getSuffixTokens();

    // If one of the versions has no suffix, it's greater!
    if (tokens1.length == 0 && tokens2.length > 0) return true;
    if (tokens2.length == 0 && tokens1.length > 0) return false;

    // Let's see if one of suffixes is greater than the other
    int i = 0;
    while (i < tokens1.length && i < tokens2.length) {
      int cmp;
      try {
        // Trying to resolve the suffix part with an integer
        int t1 = Integer.valueOf(tokens1[i]);
        int t2 = Integer.valueOf(tokens2[i]);
        cmp = t1 - t2;
      } catch (NumberFormatException e) {
        // Else, do a string comparison
        cmp = tokens1[i].compareToIgnoreCase(tokens2[i]);
      }
      if (cmp < 0) return false;
      else if (cmp > 0) return true;
      i++;
    }

    // If one of the versions has some remaining suffixes, it's greater
    return tokens1.length > tokens2.length;
  }

  /**
   * @see #isGreaterThanOrEqualTo(Semver)
   *
   * @param version the version to compare
   *
   * @return true if the current version is greater than or equal to the provided version
   */
  public boolean isGreaterThanOrEqualTo(String version) {
    return this.isGreaterThanOrEqualTo(new Semver(version, this.type));
  }

  /**
   * Checks if the version is greater than or equal to another version
   *
   * @param version the version to compare
   *
   * @return true if the current version is greater than or equal to the provided version
   */
  public boolean isGreaterThanOrEqualTo(Semver version) {
    return this.isGreaterThan(version) || this.isEquivalentTo(version);
  }

  /**
   * @see #isLowerThan(Semver)
   *
   * @param version the version to compare
   *
   * @return true if the current version is lower than the provided version
   */
  public boolean isLowerThan(String version) {
    return this.isLowerThan(new Semver(version, this.type));
  }

  /**
   * Checks if the version is lower than another version
   *
   * @param version the version to compare
   *
   * @return true if the current version is lower than the provided version
   */
  public boolean isLowerThan(Semver version) {
    return !this.isGreaterThan(version) && !this.isEquivalentTo(version);
  }

  /**
   * @see #isLowerThanOrEqualTo(Semver)
   *
   * @param version the version to compare
   *
   * @return true if the current version is lower than or equal to the provided version
   */
  public boolean isLowerThanOrEqualTo(String version) {
    return this.isLowerThanOrEqualTo(new Semver(version, this.type));
  }

  /**
   * Checks if the version is lower than or equal to another version
   *
   * @param version the version to compare
   *
   * @return true if the current version is lower than or equal to the provided version
   */
  public boolean isLowerThanOrEqualTo(Semver version) {
    return !this.isGreaterThan(version);
  }

  /**
   * @see #isEquivalentTo(Semver)
   *
   * @param version the version to compare
   *
   * @return true if the current version equals the provided version (build excluded)
   */
  public boolean isEquivalentTo(String version) {
    return this.isEquivalentTo(new Semver(version, this.type));
  }

  /**
   * Checks if the version equals another version, without taking the build into account.
   *
   * @param version the version to compare
   *
   * @return true if the current version equals the provided version (build excluded)
   */
  public boolean isEquivalentTo(Semver version) {
    // Get versions without build
    Semver sem1 = this.getBuild() == null ? this : new Semver(this.getValue().replace("+" + this.getBuild(), ""));
    Semver sem2 = version.getBuild() == null ? version : new Semver(version.getValue().replace("+" + version.getBuild(), ""));
    // Compare those new versions
    return sem1.isEqualTo(sem2);
  }

  /**
   * @see #isEqualTo(Semver)
   *
   * @param version the version to compare
   *
   * @return true if the current version equals the provided version
   */
  public boolean isEqualTo(String version) {
    return this.isEqualTo(new Semver(version, this.type));
  }

  /**
   * Checks if the version equals another version
   *
   * @param version the version to compare
   *
   * @return true if the current version equals the provided version
   */
  public boolean isEqualTo(Semver version) {
    if (this.type == SemverType.NPM) {
      if (!this.getMajor().equals(version.getMajor())) return false;
      if (version.getMinor() == null) return true;
      if (!this.getMinor().equals(version.getMinor())) return false;
      if (version.getPatch() == null) return true;
    }

    return this.equals(version);
  }

  /**
   * Determines if the current version is stable or not.
   * Stable version have a major version number strictly positive and no suffix tokens.
   *
   * @return true if the current version is stable
   */
  public boolean isStable() {
    return (this.getMajor() != null && this.getMajor() > 0) &&
        (this.getSuffixTokens() == null || this.getSuffixTokens().length == 0);
  }

  /**
   * @see #diff(Semver)
   *
   * @param version the version to compare
   *
   * @return the greatest difference
   */
  public VersionDiff diff(String version) {
    return this.diff(new Semver(version, this.type));
  }

  /**
   * Returns the greatest difference between 2 versions.
   * For example, if the current version is "1.2.3" and compared version is "1.3.0", the biggest difference
   * is the 'MINOR' number.
   *
   * @param version the version to compare
   *
   * @return the greatest difference
   */
  public VersionDiff diff(Semver version) {
    if (!Objects.equals(this.major, version.getMajor())) return VersionDiff.MAJOR;
    if (!Objects.equals(this.minor, version.getMinor())) return VersionDiff.MINOR;
    if (!Objects.equals(this.patch, version.getPatch())) return VersionDiff.PATCH;
    if (!areSameSuffixes(version.getSuffixTokens())) return VersionDiff.SUFFIX;
    if (!Objects.equals(this.build, version.getBuild())) return VersionDiff.BUILD;
    return VersionDiff.NONE;
  }

  private boolean areSameSuffixes(String[] suffixTokens) {
    if (this.suffixTokens == null && suffixTokens == null) return true;
    else if (this.suffixTokens == null || suffixTokens == null) return false;
    else if (this.suffixTokens.length != suffixTokens.length) return false;
    for (int i = 0; i < this.suffixTokens.length; i++) {
      if (!this.suffixTokens[i].equals(suffixTokens[i])) return false;
    }
    return true;
  }

  public Semver toStrict() {
    Integer minor = this.minor != null ? this.minor : 0;
    Integer patch = this.patch != null ? this.patch : 0;
    return Semver.create(SemverType.STRICT, this.major, minor, patch, this.suffixTokens, this.build);
  }

  public Semver withIncMajor() {
    return this.withIncMajor(1);
  }

  public Semver withIncMajor(int increment) {
    return this.withInc(increment, 0, 0);
  }

  public Semver withIncMinor() {
    return this.withIncMinor(1);
  }

  public Semver withIncMinor(int increment) {
    return this.withInc(0, increment, 0);
  }

  public Semver withIncPatch() {
    return this.withIncPatch(1);
  }

  public Semver withIncPatch(int increment) {
    return this.withInc(0, 0, increment);
  }

  private Semver withInc(int majorInc, int minorInc, int patchInc) {
    Integer minor = this.minor;
    Integer patch = this.patch;
    if (this.minor != null) {
      minor += minorInc;
    }
    if (this.patch != null) {
      patch += patchInc;
    }
    return with(this.major + majorInc, minor, patch, true, true);
  }

  public Semver withClearedSuffix() {
    return with(this.major, this.minor, this.patch, false, true);
  }

  public Semver withClearedBuild() {
    return with(this.major, this.minor, this.patch, true, false);
  }

  public Semver withClearedSuffixAndBuild() {
    return with(this.major, this.minor, this.patch, false, false);
  }

  public Semver withSuffix(String suffix) {
    return with(this.major, this.minor, this.patch, suffix.split("\\."), this.build);
  }

  public Semver withBuild(String build) {
    return with(this.major, this.minor, this.patch, this.suffixTokens, build);
  }

  public Semver nextMajor() {
    return with(this.major + 1, 0, 0, false, false);
  }

  public Semver nextMinor() {
    return with(this.major, this.minor + 1, 0, false, false);
  }

  public Semver nextPatch() {
    return with(this.major, this.minor, this.patch + 1, false, false);
  }

  private Semver with(int major, Integer minor, Integer patch, boolean suffix, boolean build) {
    minor = this.minor != null ? minor : null;
    patch = this.patch != null ? patch : null;
    String buildStr = build ? this.build : null;
    String[] suffixTokens = suffix ? this.suffixTokens : null;
    return Semver.create(this.type, major, minor, patch, suffixTokens, buildStr);
  }

  private Semver with(int major, Integer minor, Integer patch, String[] suffixTokens, String build) {
    minor = this.minor != null ? minor : null;
    patch = this.patch != null ? patch : null;
    return Semver.create(this.type, major, minor, patch, suffixTokens, build);
  }

  private static Semver create(SemverType type, int major, Integer minor, Integer patch, String[] suffix, String build) {
    StringBuilder sb = new StringBuilder()
        .append(major);
    if (minor != null) {
      sb.append(".").append(minor);
    }
    if (patch != null) {
      sb.append(".").append(patch);
    }
    if (suffix != null) {
      boolean first = true;
      for (String suffixToken : suffix) {
        if (first) {
          sb.append("-");
          first = false;
        } else {
          sb.append(".");
        }
        sb.append(suffixToken);
      }
    }
    if (build != null) {
      sb.append("+").append(build);
    }

    return new Semver(sb.toString(), type);
  }

  @Override public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof Semver)) return false;
    Semver version = (Semver) o;
    return value.equals(version.value);
  }

  @Override public int hashCode() {
    return value.hashCode();
  }

  @Override public int compareTo(Semver version) {
    if (this.isGreaterThan(version)) return 1;
    else if(this.isLowerThan(version)) return -1;
    return 0;
  }

  @Override public String toString() {
    return this.getValue();
  }

  /**
   * Get the original value as a string
   *
   * @return the original string passed in the constructor
   */
  public String getOriginalValue() {
    return originalValue;
  }

  /**
   * Returns the version as a String
   *
   * @return the version as a String
   */
  public String getValue() {
    return value;
  }

  /**
   * Returns the major part of the version.
   * Example: for "1.2.3" = 1
   *
   * @return the major part of the version
   */
  public Integer getMajor() {
    return this.major;
  }

  /**
   * Returns the minor part of the version.
   * Example: for "1.2.3" = 2
   *
   * @return the minor part of the version
   */
  public Integer getMinor() {
    return this.minor;
  }

  /**
   * Returns the patch part of the version.
   * Example: for "1.2.3" = 3
   *
   * @return the patch part of the version
   */
  public Integer getPatch() {
    return this.patch;
  }

  /**
   * Returns the suffix of the version.
   * Example: for "1.2.3-beta.4+sha98450956" = {"beta", "4"}
   *
   * @return the suffix of the version
   */
  public String[] getSuffixTokens() {
    return suffixTokens;
  }

  /**
   * Returns the build of the version.
   * Example: for "1.2.3-beta.4+sha98450956" = "sha98450956"
   *
   * @return the build of the version
   */
  public String getBuild() {
    return build;
  }

  public SemverType getType() {
    return type;
  }

  /**
   * The types of diffs between two versions.
   */
  public enum VersionDiff {
    NONE, MAJOR, MINOR, PATCH, SUFFIX, BUILD
  }

  /**
   * The different types of supported version systems.
   */
  public enum SemverType {
    /**
     * The default type of version.
     * Major, minor and patch parts are required.
     * Suffixes and build are optional.
     */
    STRICT,

    /**
     * Major part is required.
     * Minor, patch, suffixes and build are optional.
     */
    LOOSE,

    /**
     * Follows the rules of NPM.
     * Supports ^, x, *, ~, and more.
     * See https://github.com/npm/node-semver
     */
    NPM,

    /**
     * Follows the rules of Cocoapods.
     * Supports optimistic and comparison operators
     * See https://guides.cocoapods.org/using/the-podfile.html
     */
    COCOAPODS,

    /**
     * Follows the rules of ivy.
     * Supports dynamic parts (eg: 4.2.+) and ranges
     * See http://ant.apache.org/ivy/history/latest-milestone/ivyfile/dependency.html
     */
    IVY
  }
}
