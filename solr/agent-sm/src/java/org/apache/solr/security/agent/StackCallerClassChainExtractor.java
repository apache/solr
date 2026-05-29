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
package org.apache.solr.security.agent;

import java.lang.StackWalker.StackFrame;
import java.util.Collection;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Extracts the set of all non-hidden declaring classes from the current call stack.
 *
 * <p>This file was derived from the OpenSearch project and modified. See {@code NOTICE.txt} for
 * attribution.
 */
public final class StackCallerClassChainExtractor
    implements Function<Stream<StackFrame>, Collection<Class<?>>> {

  public static final StackCallerClassChainExtractor INSTANCE =
      new StackCallerClassChainExtractor();

  private StackCallerClassChainExtractor() {}

  @Override
  public Collection<Class<?>> apply(Stream<StackFrame> frames) {
    return cast(frames);
  }

  @SuppressWarnings("unchecked")
  private static <A> Set<A> cast(Stream<StackFrame> frames) {
    return (Set<A>)
        frames
            .map(StackFrame::getDeclaringClass)
            .filter(c -> !c.isHidden())
            .collect(Collectors.toSet());
  }
}
