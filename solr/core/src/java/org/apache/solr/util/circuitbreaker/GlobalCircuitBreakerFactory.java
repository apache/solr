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

package org.apache.solr.util.circuitbreaker;

import org.apache.solr.core.CoreContainer;

public class GlobalCircuitBreakerFactory {
  private final CoreContainer coreContainerOptional;

  public GlobalCircuitBreakerFactory(CoreContainer coreContainerOptional) {
    this.coreContainerOptional = coreContainerOptional;
  }

  public CircuitBreaker create(String className) throws Exception {
    Class<?> cl = Class.forName(className);

    if (CircuitBreaker.class.isAssignableFrom(cl)) {
      Class<? extends CircuitBreaker> breakerClass = cl.asSubclass(CircuitBreaker.class);

      try {
        // try with 0 arg constructor
        return breakerClass.getDeclaredConstructor().newInstance();
      } catch (NoSuchMethodException e) {
        // otherwise, the CircuitBreaker requires a CoreContainer, so let's give it one
        if (coreContainerOptional != null) {
          return breakerClass
              .getDeclaredConstructor(CoreContainer.class)
              .newInstance(coreContainerOptional);
        } else {
          throw new IllegalArgumentException(
              "The CircuitBreaker subclass requires a CoreContainer, but it was not provided");
        }
      }
    } else {
      throw new IllegalArgumentException(
          "Class " + className + " is not a subclass of CircuitBreaker");
    }
  }
}
