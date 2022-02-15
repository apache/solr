package org.apache.solr.common.cloud;

import org.apache.solr.common.SolrException;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import java.util.Collections;
import java.util.List;

public class PerReplicaStatesFetcher {
    /**
     * Fetch the latest {@link PerReplicaStates} . It fetches data after checking the {@link Stat#getCversion()} of state.json.
     * If this is not modified, the same object is returned
     */
    public static PerReplicaStates fetch(String path, SolrZkClient zkClient, PerReplicaStates current) {
      try {
        if (current != null) {
          Stat stat = zkClient.exists(current.path, null, true);
          if (stat == null) return new PerReplicaStates(path, -1, Collections.emptyList());
          if (current.cversion == stat.getCversion()) return current;// not modifiedZkStateReaderTest
        }
        Stat stat = new Stat();
        List<String> children = zkClient.getChildren(path, null, stat, true);
        return new PerReplicaStates(path, stat.getCversion(), Collections.unmodifiableList(children));
      } catch (KeeperException e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error fetching per-replica states", e);
      } catch (InterruptedException e) {
        SolrZkClient.checkInterrupted(e);
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Thread interrupted when loading per-replica states from " + path, e);
      }
    }
}
