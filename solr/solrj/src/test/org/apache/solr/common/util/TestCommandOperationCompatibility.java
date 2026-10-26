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
package org.apache.solr.common.util;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Set;
import org.apache.solr.SolrTestCase;
import org.junit.Test;

/** Checks the public legacy command-reading signatures without requiring them at compile time. */
@SuppressWarnings("deprecation")
public class TestCommandOperationCompatibility extends SolrTestCase {

  @Test
  public void testReadCommandsTwoArgumentSignature() throws Exception {
    Method method =
        CommandOperation.class.getMethod("readCommands", Iterable.class, NamedList.class);
    NamedList<Object> response = new NamedList<>();
    assertCommands(method, response, method.invoke(null, streams(), response));
  }

  @Test
  public void testReadCommandsThreeArgumentSignature() throws Exception {
    Method method =
        CommandOperation.class.getMethod(
            "readCommands", Iterable.class, NamedList.class, Set.class);
    NamedList<Object> response = new NamedList<>();
    assertCommands(
        method, response, method.invoke(null, streams(), response, Set.of("set-property")));
  }

  private static List<ContentStream> streams() {
    return List.of(
        new ContentStreamBase.StringStream(
            "{\"set-property\":{\"name\":\"legacy\"}}", "application/json"));
  }

  private static void assertCommands(Method method, NamedList<Object> response, Object result) {
    assertEquals(List.class, method.getReturnType());
    assertTrue("The legacy method must return a list", result instanceof List<?>);
    List<?> commands = (List<?>) result;
    assertEquals(1, commands.size());
    assertTrue(commands.get(0) instanceof CommandOperation);
    CommandOperation command = (CommandOperation) commands.get(0);
    assertEquals("set-property", command.name);
    assertEquals("legacy", command.getStr("name"));
    assertEquals(0, response.size());
  }
}
