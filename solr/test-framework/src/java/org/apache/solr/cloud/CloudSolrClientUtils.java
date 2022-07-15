package org.apache.solr.cloud;

import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.ZkClientClusterStateProvider;
import org.apache.solr.common.cloud.CollectionStatePredicate;
import org.apache.solr.common.cloud.CollectionStateWatcher;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.DocCollectionWatcher;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;

import org.apache.solr.common.cloud.ZkStateReader;

public class CloudSolrClientUtils {

    public static ZkClientClusterStateProvider assertZKStateProvider(CloudSolrClient cloudSolrClient) {
        if (cloudSolrClient.getClusterStateProvider() instanceof ZkClientClusterStateProvider) {
            return (ZkClientClusterStateProvider) cloudSolrClient.getClusterStateProvider();
        }
        throw new IllegalArgumentException("This client does not use ZK");
    }

    /**
     * Block until a CollectionStatePredicate returns true, or the wait times out
     *
     * <p>Note that the predicate may be called again even after it has returned true, so implementors
     * should avoid changing state within the predicate call itself.
     *
     * <p>This implementation utilizes {@link CollectionStateWatcher} internally. Callers that don't
     * care about liveNodes are encouraged to use a {@link DocCollection} {@link Predicate} instead
     *
     * @see ZkStateReader#waitForState(String, long, TimeUnit, Predicate)
     * @see #registerCollectionStateWatcher
     * @param cloudSolrClient
     * @param collection the collection to watch
     * @param wait how long to wait
     * @param unit the units of the wait parameter
     * @param predicate a {@link CollectionStatePredicate} to check the collection state
     * @throws InterruptedException on interrupt
     * @throws TimeoutException on timeout
     */
    public static void waitForState(CloudSolrClient cloudSolrClient,
                                    String collection, long wait, TimeUnit unit, CollectionStatePredicate predicate)
        throws InterruptedException, TimeoutException {
      cloudSolrClient.getClusterStateProvider().connect();
      assertZKStateProvider(cloudSolrClient).getZkStateReader().waitForState(collection, wait, unit, predicate);
    }

    /**
     * Block until a Predicate returns true, or the wait times out
     *
     * <p>Note that the predicate may be called again even after it has returned true, so implementors
     * should avoid changing state within the predicate call itself.
     *
     * @see #registerDocCollectionWatcher
     * @param cloudSolrClient
     * @param collection the collection to watch
     * @param wait how long to wait
     * @param unit the units of the wait parameter
     * @param predicate a {@link Predicate} to test against the {@link DocCollection}
     * @throws InterruptedException on interrupt
     * @throws TimeoutException on timeout
     */
    public static void waitForState(CloudSolrClient cloudSolrClient,
                                    String collection, long wait, TimeUnit unit, Predicate<DocCollection> predicate)
        throws InterruptedException, TimeoutException {
      cloudSolrClient.getClusterStateProvider().connect();
      assertZKStateProvider(cloudSolrClient).getZkStateReader().waitForState(collection, wait, unit, predicate);
    }

    /**
     * Register a CollectionStateWatcher to be called when the cluster state for a collection changes
     * <em>or</em> the set of live nodes changes.
     *
     * <p>The Watcher will automatically be removed when it's <code>onStateChanged</code> returns
     * <code>true</code>
     *
     * <p>This implementation utilizes {@link ZkStateReader#registerCollectionStateWatcher}
     * internally. Callers that don't care about liveNodes are encouraged to use a {@link
     * DocCollectionWatcher} instead
     *
     * @see ZkStateReader#registerDocCollectionWatcher(String, DocCollectionWatcher)
     * @see ZkStateReader#registerCollectionStateWatcher
     * @param cloudSolrClient
     * @param collection the collection to watch
     * @param watcher a watcher that will be called when the state changes
     */
    public static void registerCollectionStateWatcher(CloudSolrClient cloudSolrClient, String collection, CollectionStateWatcher watcher) {
        cloudSolrClient.getClusterStateProvider().connect();
        assertZKStateProvider(cloudSolrClient).getZkStateReader().registerCollectionStateWatcher(collection, watcher);
    }

    /**
     * Register a DocCollectionWatcher to be called when the cluster state for a collection changes.
     *
     * <p>The Watcher will automatically be removed when it's <code>onStateChanged</code> returns
     * <code>true</code>
     *
     * @param cloudSolrClient
     * @param collection the collection to watch
     * @param watcher a watcher that will be called when the state changes
     */
    public static void registerDocCollectionWatcher(CloudSolrClient cloudSolrClient, String collection, DocCollectionWatcher watcher) {
      cloudSolrClient.getClusterStateProvider().connect();
      assertZKStateProvider(cloudSolrClient).getZkStateReader().registerDocCollectionWatcher(collection, watcher);
    }
}
