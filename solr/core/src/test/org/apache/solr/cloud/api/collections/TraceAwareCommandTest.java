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
package org.apache.solr.cloud.api.collections;

import static org.apache.solr.common.params.CommonParams.NAME;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Context;
import io.opentelemetry.sdk.testing.junit4.OpenTelemetryRule;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.solr.SolrTestCase;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.params.CollectionParams.CollectionAction;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.util.tracing.TraceUtils;
import org.junit.ClassRule;
import org.junit.Test;

/** Tests {@link CollApiCmds.TraceAwareCommand}. */
public class TraceAwareCommandTest extends SolrTestCase {

  @ClassRule public static OpenTelemetryRule otelRule = OpenTelemetryRule.create();

  @Test
  public void testCommandDoesNotSeeTraceContext() throws Exception {
    Map<String, Object> props = new HashMap<>(Map.of(NAME, "collection1"));
    Span caller = otelRule.getOpenTelemetry().getTracer("test").spanBuilder("caller").startSpan();
    Context.current().with(caller).wrap(() -> TraceUtils.injectTraceContext(props)).run();
    caller.end();
    ZkNodeProps message = new ZkNodeProps(props);
    assertNotEquals(Set.of(NAME), message.keySet());

    AtomicReference<ZkNodeProps> seen = new AtomicReference<>();
    new CollApiCmds.TraceAwareCommand((ctx, m, results) -> seen.set(m))
        .call(new AdminCmdContext(CollectionAction.RELOAD), message, new NamedList<>());
    assertEquals(Set.of(NAME), seen.get().keySet());
  }
}
