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
package org.apache.solr.client.solrj.impl;

/**
 * Interface to abstract time measurements for easier testing. This allows tests to control time
 * advancement rather than relying on actual wall clock time.
 */
public interface TimeSource {

  /**
   * Returns the current value of the running Java Virtual Machine's high-resolution time source, in
   * nanoseconds.
   *
   * @return the current value of the running Java Virtual Machine's high-resolution time source.
   */
  long nanoTime();

  /** Default implementation that uses System.nanoTime(). */
  TimeSource SYSTEM = System::nanoTime;
}
