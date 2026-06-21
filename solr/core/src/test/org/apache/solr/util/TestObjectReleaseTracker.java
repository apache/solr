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
package org.apache.solr.util;

import org.apache.lucene.util.TestRuleLimitSysouts.Limit;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.solr.common.util.ObjectReleaseTrackerTestImpl;
import org.junit.Test;


@Limit(bytes=150000) // raise limit as this writes to sys err
public class TestObjectReleaseTracker extends SolrTestCaseJ4 {
  
  @Test
  public void testObjectReleaseTracker() {

    ObjectReleaseTracker objectReleaseTracker = new ObjectReleaseTrackerTestImpl();
    objectReleaseTracker.track(new Object());
    objectReleaseTracker.release(new Object());
    assertNotNull(objectReleaseTracker.checkEmpty());
    objectReleaseTracker.clear();
    Object obj = new Object();
    objectReleaseTracker.track(obj);
    objectReleaseTracker.release(obj);
    assertNull(objectReleaseTracker.checkEmpty());
    objectReleaseTracker.clear();
    
    Object obj1 = new Object();
    objectReleaseTracker.track(obj1);
    Object obj2 = new Object();
    objectReleaseTracker.track(obj2);
    Object obj3 = new Object();
    objectReleaseTracker.track(obj3);
    
    objectReleaseTracker.release(obj1);
    objectReleaseTracker.release(obj2);
    objectReleaseTracker.release(obj3);
    assertNull(objectReleaseTracker.checkEmpty());
    objectReleaseTracker.clear();
    
    objectReleaseTracker.track(obj1);
    objectReleaseTracker.track(obj2);
    objectReleaseTracker.track(obj3);
    
    objectReleaseTracker.release(obj1);
    objectReleaseTracker.release(obj2);
    // ObjectReleaseTracker.release(obj3);
    assertNotNull(objectReleaseTracker.checkEmpty());
    objectReleaseTracker.clear();
    assertNull(objectReleaseTracker.checkEmpty());
    objectReleaseTracker.clear();
  }
}
