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

import java.text.ParseException;
import java.util.Locale;

/**
 * Use by certain classes to match version compatibility across releases of Solr.
 */
public final class SolrVersion {

  /** @deprecated (9.0.0) Use latest */
  @Deprecated public static final SolrVersion SOLR_8_11_1 = new SolrVersion(8, 11, 1);

  /**
   * Latest current version of Solr
   */
  public static final SolrVersion SOLR_9_0_0 = new SolrVersion(9, 0, 0);

  // To add a new version:
  //  * Only add above this comment
  //  * If the new version is the newest, change LATEST below and deprecate the previous LATEST

  /**
   * <b>WARNING</b>: Be careful where you use this constant. Usually you should use the exact version constants
   */
  public static final SolrVersion LATEST = SOLR_9_0_0;

  /**
   * Parse a version number of the form {@code "major.minor.bugfix.prerelease"}.
   *
   * <p>Part {@code ".bugfix"} and part {@code ".prerelease"} are optional. Note that this is
   * forwards compatible: the parsed version does not have to exist as a constant.
   */
  public static SolrVersion parse(String version) throws ParseException {

    StrictStringTokenizer tokens = new StrictStringTokenizer(version, '.');
    if (!tokens.hasMoreTokens()) {
      throw new ParseException(
          "Version is not in form major.minor.bugfix(.prerelease) (got: " + version + ")", 0);
    }

    int major;
    String token = tokens.nextToken();
    try {
      major = Integer.parseInt(token);
    } catch (NumberFormatException nfe) {
      ParseException p =
          new ParseException(
              "Failed to parse major version from \"" + token + "\" (got: " + version + ")", 0);
      p.initCause(nfe);
      throw p;
    }

    if (!tokens.hasMoreTokens()) {
      throw new ParseException(
          "Version is not in form major.minor.bugfix(.prerelease) (got: " + version + ")", 0);
    }

    int minor;
    token = tokens.nextToken();
    try {
      minor = Integer.parseInt(token);
    } catch (NumberFormatException nfe) {
      ParseException p =
          new ParseException(
              "Failed to parse minor version from \"" + token + "\" (got: " + version + ")", 0);
      p.initCause(nfe);
      throw p;
    }

    int bugfix = 0;
    int prerelease = 0;
    if (tokens.hasMoreTokens()) {

      token = tokens.nextToken();
      try {
        bugfix = Integer.parseInt(token);
      } catch (NumberFormatException nfe) {
        ParseException p =
            new ParseException(
                "Failed to parse bugfix version from \"" + token + "\" (got: " + version + ")", 0);
        p.initCause(nfe);
        throw p;
      }

      if (tokens.hasMoreTokens()) {
        token = tokens.nextToken();
        try {
          prerelease = Integer.parseInt(token);
        } catch (NumberFormatException nfe) {
          ParseException p =
              new ParseException(
                  "Failed to parse prerelease version from \""
                      + token
                      + "\" (got: "
                      + version
                      + ")",
                  0);
          p.initCause(nfe);
          throw p;
        }
        if (prerelease == 0) {
          throw new ParseException(
              "Invalid value "
                  + prerelease
                  + " for prerelease; should be 1 or 2 (got: "
                  + version
                  + ")",
              0);
        }

        if (tokens.hasMoreTokens()) {
          // Too many tokens!
          throw new ParseException(
              "Version is not in form major.minor.bugfix(.prerelease) (got: " + version + ")", 0);
        }
      }
    }

    try {
      return new SolrVersion(major, minor, bugfix, prerelease);
    } catch (IllegalArgumentException iae) {
      ParseException pe =
          new ParseException(
              "failed to parse version string \"" + version + "\": " + iae.getMessage(), 0);
      pe.initCause(iae);
      throw pe;
    }
  }

  /**
   * Parse the given version number as a constant or dot based version.
   *
   * <p>This method allows to use {@code "SOLR_X_Y"} constant names, or version numbers in the
   * format {@code "x.y.z"}.
   */
  public static SolrVersion parseLeniently(String version) throws ParseException {
    String versionOrig = version;
    version = version.toUpperCase(Locale.ROOT);
    switch (version) {
      case "LATEST":
      case "SOLR_CURRENT":
        return LATEST;
      default:
        version =
            version
                .replaceFirst("^SOLR_(\\d+)_(\\d+)_(\\d+)$", "$1.$2.$3")
                .replaceFirst("^SOLR_(\\d+)_(\\d+)$", "$1.$2.0")
                .replaceFirst("^SOLR_(\\d)(\\d)$", "$1.$2.0");
        try {
          return parse(version);
        } catch (ParseException pe) {
          ParseException pe2 =
              new ParseException(
                  "failed to parse lenient version string \""
                      + versionOrig
                      + "\": "
                      + pe.getMessage(),
                  0);
          pe2.initCause(pe);
          throw pe2;
        }
    }
  }

  /**
   * Returns a new version based on raw numbers
   */
  public static SolrVersion fromBits(int major, int minor, int bugfix) {
    return new SolrVersion(major, minor, bugfix);
  }

  /** Major version, the difference between stable and trunk */
  public final int major;
  /** Minor version, incremented within the stable branch */
  public final int minor;
  /** Bugfix number, incremented on release branches */
  public final int bugfix;
  /** Prerelease version, currently 0 (alpha), 1 (beta), or 2 (final) */
  public final int prerelease;

  // stores the version pieces, with most significant pieces in high bits
  // ie:  | 1 byte | 1 byte | 1 byte |   2 bits   |
  //         major   minor    bugfix   prerelease
  private final int encodedValue;

  private SolrVersion(int major, int minor, int bugfix) {
    this(major, minor, bugfix, 0);
  }

  private SolrVersion(int major, int minor, int bugfix, int prerelease) {
    this.major = major;
    this.minor = minor;
    this.bugfix = bugfix;
    this.prerelease = prerelease;
    // NOTE: do not enforce major version so we remain future proof, except to
    // make sure it fits in the 8 bits we encode it into:
    if (major > 255 || major < 0) {
      throw new IllegalArgumentException("Illegal major version: " + major);
    }
    if (minor > 255 || minor < 0) {
      throw new IllegalArgumentException("Illegal minor version: " + minor);
    }
    if (bugfix > 255 || bugfix < 0) {
      throw new IllegalArgumentException("Illegal bugfix version: " + bugfix);
    }
    if (prerelease > 2 || prerelease < 0) {
      throw new IllegalArgumentException("Illegal prerelease version: " + prerelease);
    }
    if (prerelease != 0 && (minor != 0 || bugfix != 0)) {
      throw new IllegalArgumentException(
          "Prerelease version only supported with major release (got prerelease: "
              + prerelease
              + ", minor: "
              + minor
              + ", bugfix: "
              + bugfix
              + ")");
    }

    encodedValue = major << 18 | minor << 10 | bugfix << 2 | prerelease;

    assert encodedIsValid();
  }

  /** Returns true if this version is the same or after the version from the argument. */
  public boolean onOrAfter(SolrVersion other) {
    return encodedValue >= other.encodedValue;
  }

  @Override
  public String toString() {
    if (prerelease == 0) {
      return "" + major + "." + minor + "." + bugfix;
    }
    return "" + major + "." + minor + "." + bugfix + "." + prerelease;
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof SolrVersion && ((SolrVersion) o).encodedValue == encodedValue;
  }

  // Used only by assert:
  private boolean encodedIsValid() {
    assert major == ((encodedValue >>> 18) & 0xFF);
    assert minor == ((encodedValue >>> 10) & 0xFF);
    assert bugfix == ((encodedValue >>> 2) & 0xFF);
    assert prerelease == (encodedValue & 0x03);
    return true;
  }

  @Override
  public int hashCode() {
    return encodedValue;
  }

  /**
   * Copied from Lucene
   */
  static final class StrictStringTokenizer {

    public StrictStringTokenizer(String s, char delimiter) {
      this.s = s;
      this.delimiter = delimiter;
    }

    public String nextToken() {
      if (pos < 0) {
        throw new IllegalStateException("no more tokens");
      }

      int pos1 = s.indexOf(delimiter, pos);
      String s1;
      if (pos1 >= 0) {
        s1 = s.substring(pos, pos1);
        pos = pos1 + 1;
      } else {
        s1 = s.substring(pos);
        pos = -1;
      }

      return s1;
    }

    public boolean hasMoreTokens() {
      return pos >= 0;
    }

    private final String s;
    private final char delimiter;
    private int pos;
  }

}