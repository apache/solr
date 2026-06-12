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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Suppresses forbidden-API checker violations for the annotated element. The {@code reason} field
 * must explain why the forbidden API is necessary here.
 *
 * <p>This local copy mirrors the semantics of {@code org.apache.solr.common.util.SuppressForbidden}
 * and {@code org.apache.lucene.util.SuppressForbidden}. The Gradle forbidden-apis plugin matches
 * any annotation named {@code SuppressForbidden} regardless of package.
 */
@Retention(RetentionPolicy.CLASS)
@Target({ElementType.TYPE, ElementType.METHOD, ElementType.CONSTRUCTOR})
public @interface SuppressForbidden {
  /** Explanation of why the forbidden API is necessary at this call site. */
  String reason();
}
