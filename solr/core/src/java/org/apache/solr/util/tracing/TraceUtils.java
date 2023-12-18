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
package org.apache.solr.util.tracing;

import io.opentracing.Span;
import io.opentracing.noop.NoopSpan;
import io.opentracing.tag.Tags;
import java.util.function.Consumer;
import org.apache.solr.request.SolrQueryRequest;

/** Utilities for distributed tracing. */
public class TraceUtils {

  public static void setDbInstance(SolrQueryRequest req, String coreOrColl) {
    if (req != null && coreOrColl != null) {
      ifNotNoop(req.getSpan(), (span) -> span.setTag(Tags.DB_INSTANCE, coreOrColl));
    }
  }

  public static void ifNotNoop(Span span, Consumer<Span> consumer) {
    if (span != null && !(span instanceof NoopSpan)) {
      consumer.accept(span);
    }
  }
}
