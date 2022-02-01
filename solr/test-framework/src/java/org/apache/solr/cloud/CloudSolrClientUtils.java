package org.apache.solr.cloud;

import org.apache.solr.client.solrj.impl.BaseCloudSolrClient;
import org.apache.solr.client.solrj.impl.ZkClientClusterStateProvider;
import org.apache.solr.common.cloud.CollectionStatePredicate;
import org.apache.solr.common.cloud.CollectionStateWatcher;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.DocCollectionWatcher;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;

public class CloudSolrClientUtils {
    public static ZkClientClusterStateProvider assertZKStateProvider(BaseCloudSolrClient client) {
      if (client.getClusterStateProvider() instanceof ZkClientClusterStateProvider) {
        return (ZkClientClusterStateProvider) client.getClusterStateProvider();
      }
      throw new IllegalArgumentException("This client does not use ZK");

    }

    // nocommit : change = to setters in the following methods

    /** Set the timeout to the zookeeper ensemble in ms */
    public static void setZkClientTimeout(BaseCloudSolrClient client, int zkClientTimeout) {
      assertZKStateProvider(client).zkClientTimeout = zkClientTimeout;
    }

    /**
     * @return the zkHost value used to connect to zookeeper.
     */
    public static String getZkHost(BaseCloudSolrClient baseCloudSolrClient) {
      return assertZKStateProvider(baseCloudSolrClient).zkHost;
    }

    /** Set the connect timeout to the zookeeper ensemble in ms */
    public static void setZkConnectTimeout(BaseCloudSolrClient client, int zkConnectTimeout) {
      assertZKStateProvider(client).zkConnectTimeout = zkConnectTimeout;
    }

    /**
     * Block until a CollectionStatePredicate returns true, or the wait times out
     *
     * <p>
     * Note that the predicate may be called again even after it has returned true, so
     * implementors should avoid changing state within the predicate call itself.
     * </p>
     *
     * <p>
     * This implementation utilizes {@link CollectionStateWatcher} internally.
     * Callers that don't care about liveNodes are encouraged to use a {@link DocCollection} {@link Predicate}
     * instead
     * </p>
     *
     * @see #waitForState(BaseCloudSolrClient, String, long, TimeUnit, CollectionStatePredicate)
     * @see #registerCollectionStateWatcher
     * @throws InterruptedException on interrupt
     * @throws TimeoutException     on timeout
     */
    public static void waitForState(BaseCloudSolrClient client, String collection, long wait, TimeUnit unit, CollectionStatePredicate predicate)
        throws InterruptedException, TimeoutException {
      client.getClusterStateProvider().connect();
      assertZKStateProvider(client).getZkStateReader().waitForState(collection, wait, unit, predicate);
    }

    /**
     * Block until a Predicate returns true, or the wait times out
     *
     * <p>
     * Note that the predicate may be called again even after it has returned true, so
     * implementors should avoid changing state within the predicate call itself.
     * </p>
     *
     * @see #registerDocCollectionWatcher
     * @throws InterruptedException on interrupt
     * @throws TimeoutException     on timeout
     */
    public static void waitForState(BaseCloudSolrClient client, String collection, long wait, TimeUnit unit, Predicate<DocCollection> predicate)
        throws InterruptedException, TimeoutException {
      client.getClusterStateProvider().connect();
      assertZKStateProvider(client).getZkStateReader().waitForState(collection, wait, unit, predicate);
    }

    /**
     * Register a CollectionStateWatcher to be called when the cluster state for a collection changes
     * <em>or</em> the set of live nodes changes.
     *
     * <p>
     * The Watcher will automatically be removed when it's
     * <code>onStateChanged</code> returns <code>true</code>
     * </p>
     *
     * <p>
     * This implementation utilizes {@link org.apache.solr.common.cloud.ZkStateReader#registerCollectionStateWatcher} internally.
     * Callers that don't care about liveNodes are encouraged to use a {@link DocCollectionWatcher}
     * instead
     * </p>
     *
     * @see #registerDocCollectionWatcher(BaseCloudSolrClient, String, DocCollectionWatcher)
     * @see org.apache.solr.common.cloud.ZkStateReader#registerCollectionStateWatcher
     */
    public static void registerCollectionStateWatcher(BaseCloudSolrClient client, String collection, CollectionStateWatcher watcher) {
      client.getClusterStateProvider().connect();
      assertZKStateProvider(client).getZkStateReader().registerCollectionStateWatcher(collection, watcher);
    }

    /**
     * Register a DocCollectionWatcher to be called when the cluster state for a collection changes.
     *
     * <p>
     * The Watcher will automatically be removed when it's
     * <code>onStateChanged</code> returns <code>true</code>
     * </p>
     *
     * @see org.apache.solr.common.cloud.ZkStateReader#registerDocCollectionWatcher
     */
    public static void registerDocCollectionWatcher(BaseCloudSolrClient client, String collection, DocCollectionWatcher watcher) {
      client.getClusterStateProvider().connect();
      assertZKStateProvider(client).getZkStateReader().registerDocCollectionWatcher(collection, watcher);
    }
}
