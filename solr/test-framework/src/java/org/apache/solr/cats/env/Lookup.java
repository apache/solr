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

package org.apache.solr.cats.env;

import java.util.Locale;

/**
 * The ordered chain a setting is read from: the process environment first, then the system property
 * the name maps to.
 *
 * <p>The environment is how a build task supplies a value, the system property how a developer
 * running one class from an IDE overrides it. A name is spelled once, in environment-variable form,
 * and the property follows from it — {@code CATS_OUTPUT_DIR} is also {@code -Dcats.output.dir}.
 *
 * <p>The harness' own settings are read through {@link #CATS}. The variables of the program under
 * test are read through a chain carrying that project's own prefix.
 */
public final class Lookup {

  /** The chain for the harness' own settings, which are always named {@code cats.*}. */
  public static final Lookup CATS = new Lookup("cats.");

  private final String sysPropPrefix;

  public Lookup(String sysPropPrefix) {
    this.sysPropPrefix = sysPropPrefix;
  }

  /** The system property {@code name} maps to. */
  public String sysProp(String name) {
    return sysPropPrefix + name.toLowerCase(Locale.ROOT).replace('_', '.');
  }

  /** The value of {@code name}, or null when neither channel holds one. */
  public String get(String name) {
    String fromEnv = System.getenv(name);
    if (fromEnv != null && !fromEnv.isBlank()) {
      return fromEnv;
    }
    String fromProperty = System.getProperty(sysProp(name));
    return (fromProperty == null || fromProperty.isBlank()) ? null : fromProperty;
  }
}
