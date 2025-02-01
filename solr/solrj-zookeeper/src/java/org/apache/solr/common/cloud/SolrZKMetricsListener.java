package org.apache.solr.common.cloud;

import org.apache.curator.drivers.AdvancedTracerDriver;
import org.apache.curator.drivers.EventTrace;
import org.apache.curator.drivers.OperationTrace;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.annotation.JsonProperty;
import org.apache.solr.common.util.ReflectMapWriter;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

public class SolrZKMetricsListener extends AdvancedTracerDriver implements ReflectMapWriter, CuratorListener {
  // all fields of this class are public because ReflectMapWriter requires them to be.
  // however the object itself is private and only this class can modify it

  @JsonProperty public final LongAdder watchesFired = new LongAdder();
  @JsonProperty public final LongAdder reads = new LongAdder();
  @JsonProperty public final LongAdder writes = new LongAdder();
  @JsonProperty public final LongAdder bytesRead = new LongAdder();
  @JsonProperty public final LongAdder bytesWritten = new LongAdder();

  @JsonProperty public final LongAdder multiOps = new LongAdder();

  @JsonProperty public final LongAdder cumulativeMultiOps = new LongAdder();

  @JsonProperty public final LongAdder childFetches = new LongAdder();

  @JsonProperty public final LongAdder cumulativeChildrenFetched = new LongAdder();

  @JsonProperty public final LongAdder existsChecks = new LongAdder();

  @JsonProperty public final LongAdder deletes = new LongAdder();

  /*
  This is used by curator for all operations, but we will only use it for Foreground operations.
  Background operations are handled by the eventReceived() method instead.
   */
  @Override
  public void addTrace(OperationTrace trace) {
    System.out.println("METRICS LOGGED");
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
        if (event.getChildren() != null) {
          cumulativeChildrenFetched.add(event.getChildren().size());
        }
      }
      case "CuratorMultiTransactionImpl-Foreground" -> {
        multiOps.increment();
        if (event.getOpResults() != null) {
          cumulativeMultiOps.add(event.getOpResults().size());
        }
      }
    }
  }

  /*
  This is used by Zookeeper for ConnectionState changes and retries.
  We currently do not record metrics for either.
   */
  @Override
  public void addEvent(EventTrace trace) { }

  /*
  This is used for Background operations and Watch firing.
   */
  @Override
  public void eventReceived(CuratorFramework client, CuratorEvent event) {
    System.out.println("METRICS LOGGED");
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
    }
  }

  @Override
  public void writeMap(MapWriter.EntryWriter ew) throws IOException {
    ReflectMapWriter.super.writeMap(
        new MapWriter.EntryWriter() {
          @Override
          public MapWriter.EntryWriter put(CharSequence k, Object v) throws IOException {
            if (v instanceof LongAdder) {
              ew.put(k, ((LongAdder) v).longValue());
            } else {
              ew.put(k, v);
            }
            return this;
          }
        });
  }
}
