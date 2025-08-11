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
package org.apache.solr.common.cloud;

import java.util.concurrent.atomic.LongAdder;
import org.apache.curator.drivers.AdvancedTracerDriver;
import org.apache.curator.drivers.EventTrace;
import org.apache.curator.drivers.OperationTrace;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorListener;

public class SolrZKMetricsListener extends AdvancedTracerDriver implements CuratorListener {

  final LongAdder watchesFired = new LongAdder();
  final LongAdder reads = new LongAdder();
  final LongAdder writes = new LongAdder();
  final LongAdder bytesRead = new LongAdder();
  final LongAdder bytesWritten = new LongAdder();
  final LongAdder multiOps = new LongAdder();
  final LongAdder cumulativeMultiOps = new LongAdder();
  final LongAdder childFetches = new LongAdder();
  final LongAdder cumulativeChildrenFetched = new LongAdder();
  final LongAdder existsChecks = new LongAdder();
  final LongAdder deletes = new LongAdder();

  /*
  This is used by curator for all operations, but we will only use it for Foreground operations.
  Background operations are handled by the eventReceived() method instead.
   */
  @Override
  public void addTrace(OperationTrace trace) {
    switch (trace.getName()) {
      case "CreateBuilderImpl-Foreground", "SetDataBuilderImpl-Foreground" -> {
        writes.increment();
        bytesWritten.add(trace.getRequestBytesLength());
      }
      case "DeleteBuilderImpl-Foreground" -> deletes.increment();
      case "ExistsBuilderImpl-Foreground" -> existsChecks.increment();
      case "GetDataBuilderImpl-Foreground" -> {
        reads.increment();
        bytesRead.add(trace.getResponseBytesLength());
      }
      case "GetChildrenBuilderImpl-Foreground" -> {
        childFetches.increment();
        cumulativeChildrenFetched.add(trace.getResponseChildrenCount());
      }
      case "CuratorMultiTransactionImpl-Foreground" -> {
        multiOps.increment();
        cumulativeMultiOps.add(trace.getRequestTransactionCount());
      }
      default -> {
        // NO-OP - We do not currently track metrics for these
      }
    }
  }

  /*
  This is used by Zookeeper for ConnectionState changes and retries.
  We currently do not record metrics for either.
   */
  @Override
  public void addEvent(EventTrace trace) {}

  /*
  This is used for Background operations and Watch firing.
   */
  @Override
  public void eventReceived(CuratorFramework client, CuratorEvent event) {
    switch (event.getType()) {
      case CREATE, SET_DATA -> {
        writes.increment();
        if (event.getData() != null) {
          bytesWritten.add(event.getData().length);
        }
      }
      case DELETE -> deletes.increment();
      case EXISTS -> existsChecks.increment();
      case GET_DATA -> {
        reads.increment();
        if (event.getData() != null) {
          bytesRead.add(event.getData().length);
        }
      }
      case CHILDREN -> {
        childFetches.increment();
        if (event.getChildren() != null) {
          cumulativeChildrenFetched.add(event.getChildren().size());
        }
      }
      case TRANSACTION -> {
        multiOps.increment();
        if (event.getOpResults() != null) {
          cumulativeMultiOps.add(event.getOpResults().size());
        }
      }
      case WATCHED -> watchesFired.increment();
      default -> {
        // NO-OP - We do not currently track metrics for these
      }
    }
  }

  public long getWatchesFired() {
    return watchesFired.longValue();
  }

  public long getReads() {
    return reads.longValue();
  }

  public long getWrites() {
    return writes.longValue();
  }

  public long getBytesRead() {
    return bytesRead.longValue();
  }

  public long getBytesWritten() {
    return bytesWritten.longValue();
  }

  public long getMultiOps() {
    return multiOps.longValue();
  }

  public long getCumulativeMultiOps() {
    return cumulativeMultiOps.longValue();
  }

  public long getChildFetches() {
    return childFetches.longValue();
  }

  public long getCumulativeChildrenFetched() {
    return cumulativeChildrenFetched.longValue();
  }

  public long getExistsChecks() {
    return existsChecks.longValue();
  }

  public long getDeletes() {
    return deletes.longValue();
  }
}
