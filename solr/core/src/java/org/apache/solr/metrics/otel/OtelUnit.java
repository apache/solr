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
package org.apache.solr.metrics.otel;

/**
 * Standard metric units for OpenTelemetry instruments based on UCUM (Unified Code
 * for Units of Measure).
 */
public enum OtelUnit {
  // Time units
  NANOSECONDS("ns"),
  MICROSECONDS("us"),
  MILLISECONDS("ms"),
  SECONDS("s"),
  MINUTES("min"),
  HOURS("h"),
  DAYS("d"),

  // Byte units
  BYTES("By"),
  KILOBYTES("kBy"),
  MEGABYTES("MBy"),
  GIGABYTES("GBy");

  private final String symbol;

  OtelUnit(String symbol) {
    this.symbol = symbol;
  }

  public String getSymbol() {
    return symbol;
  }

  @Override
  public String toString() {
    return symbol;
  }
}
