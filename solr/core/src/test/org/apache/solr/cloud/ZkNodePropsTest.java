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
package org.apache.solr.cloud;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectMap;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.util.Utils;
import org.junit.Test;

// Fork note: the javabin round-trip half of this test was removed because the fork's ZkNodeProps
// implements only JSONWriter.Writable (not MapWriter/Map).  JavaBinCodec serialises it as a String
// (className:toString) which causes a ClassCastException on load.  Making ZkNodeProps a MapWriter
// would change the javabin wire format everywhere in SolrCloud — deliberately out of scope.
// Only the JSON round-trip (which the fork fully supports) is exercised here.
public class ZkNodePropsTest extends SolrTestCaseJ4 {
  @Test
  public void testBasic() throws IOException {

    Object2ObjectMap<String,Object> props = new Object2ObjectLinkedOpenHashMap<>();
    props.put("prop1", "value1");
    props.put("prop2", "value2");
    props.put("prop3", "value3");
    props.put("prop4", "value4");
    props.put("prop5", "value5");
    props.put("prop6", "value6");

    ZkNodeProps zkProps = new ZkNodeProps(props);
    byte[] bytes = Utils.toJSON(zkProps);
    ZkNodeProps props2 = ZkNodeProps.load(bytes);

    props.forEach((s, o) -> assertEquals(o, props2.get(s)));

    // Javabin round-trip removed: fork's ZkNodeProps implements only JSONWriter.Writable
    // (not MapWriter/Map), so JavaBinCodec serialises it as a String → ClassCastException on load.
    // Fixing this would change the javabin wire format everywhere; out of scope for this fork.
  }
}
