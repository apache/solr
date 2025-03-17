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

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Some tests need to reach outside the classpath to get certain resources (e.g. the example
 * configuration). This class provides some paths to allow them to do this.
 *
 * @lucene.internal
 */
public class ExternalPaths {

  /**
   * The main directory path for the solr source being built if it can be determined. If it can not
   * be determined -- possibly because the current context is a client code base using the test
   * framework -- then this variable will be null.
   *
   * <p>Note that all other static paths available in this class are derived from the source home,
   * and if it is null, those paths will just be relative to 'null' and may not be meaningful.
   */
  public static final Path SOURCE_HOME = determineSourceHome();

  /**
   * @see #SOURCE_HOME
   */
  public static Path WEBAPP_HOME = SOURCE_HOME.resolve("webapp/web").toAbsolutePath();

  /**
   * @see #SOURCE_HOME
   */
  public static Path DEFAULT_CONFIGSET =
      SOURCE_HOME.resolve("server/solr/configsets/_default/conf").toAbsolutePath();

  /**
   * @see #SOURCE_HOME
   */
  public static Path TECHPRODUCTS_CONFIGSET =
      SOURCE_HOME
          .resolve("server/solr/configsets/sample_techproducts_configs/conf")
          .toAbsolutePath();

  /**
   * @see #SOURCE_HOME
   */
  public static Path SERVER_HOME = SOURCE_HOME.resolve("server/solr").toAbsolutePath();

  /**
   * Ugly, ugly hack to determine the example home without depending on the CWD this is needed for
   * example/multicore tests which reside outside the classpath. if the source home can't be
   * determined, this method returns null.
   */
  static Path determineSourceHome() {
    try {
      Path file = Path.of("solr/conf");
      if (!Files.exists(file)) {
        URL resourceUrl = ExternalPaths.class.getClassLoader().getResource("solr/conf");
        if (resourceUrl != null) {
          file = Path.of(resourceUrl.toURI());
        } else {
          // If there is no "solr/conf" in the classpath, fall back to searching from the current
          // directory.
          file = Path.of(System.getProperty("tests.src.home", "."));
        }
      }

      Path base = file.toAbsolutePath();
      while (!Files.exists(base.resolve("solr/CHANGES.txt")) && null != base) {
        base = base.getParent();
      }
      return (null == base) ? null : base.resolve("solr/").toAbsolutePath();
    } catch (Exception e) {
      // all bets are off
      return null;
    }
  }
}
