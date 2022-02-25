package org.apache.solr.cloud;

import org.apache.solr.client.solrj.impl.BaseCloudSolrClient;
import org.apache.solr.client.solrj.impl.ZkClientClusterStateProvider;
import org.apache.solr.common.cloud.*;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;

public class CloudSolrClientUtils implements Serializable {
    /**
     * @return the zkHost value used to connect to zookeeper.
     */
    public static String getZkHost(BaseCloudSolrClient client) {
        return assertZKStateProvider(client).zkHost;
    }

    /**
     * Set the connect timeout to the zookeeper ensemble in ms
     */
    public static void setZkConnectTimeout(BaseCloudSolrClient client, int zkConnectTimeout) {
        assertZKStateProvider(client).zkConnectTimeout = zkConnectTimeout;
    }

    /**
     * Set the timeout to the zookeeper ensemble in ms
     */
    public static void setZkClientTimeout(BaseCloudSolrClient client, int zkClientTimeout) {
        assertZKStateProvider(client).zkClientTimeout = zkClientTimeout;
    }

    static ZkClientClusterStateProvider assertZKStateProvider(BaseCloudSolrClient client) {
        if (client.getClusterStateProvider() instanceof ZkClientClusterStateProvider) {
            return (ZkClientClusterStateProvider) client.getClusterStateProvider();
        }
        throw new IllegalArgumentException("This client does not use ZK");

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
     * @param collection the collection to watch
     * @param wait       how long to wait
     * @param unit       the units of the wait parameter
     * @param predicate  a {@link CollectionStatePredicate} to check the collection state
     * @throws InterruptedException on interrupt
     * @throws TimeoutException     on timeout
     * @see #waitForState(BaseCloudSolrClient, String, long, TimeUnit, Predicate)
     * @see #registerCollectionStateWatcher
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
     * @param collection the collection to watch
     * @param wait       how long to wait
     * @param unit       the units of the wait parameter
     * @param predicate  a {@link Predicate} to test against the {@link DocCollection}
     * @throws InterruptedException on interrupt
     * @throws TimeoutException     on timeout
     * @see #registerDocCollectionWatcher
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
     * This implementation utilizes {@link ZkStateReader#registerCollectionStateWatcher} internally.
     * Callers that don't care about liveNodes are encouraged to use a {@link DocCollectionWatcher}
     * instead
     * </p>
     *
     * @param collection the collection to watch
     * @param watcher    a watcher that will be called when the state changes
     * @see #registerDocCollectionWatcher(BaseCloudSolrClient, String, DocCollectionWatcher)
     * @see ZkStateReader#registerCollectionStateWatcher
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
     * @param collection the collection to watch
     * @param watcher    a watcher that will be called when the state changes
     * @see ZkStateReader#registerDocCollectionWatcher
     */
    public static void registerDocCollectionWatcher(BaseCloudSolrClient client, String collection, DocCollectionWatcher watcher) {
        client.getClusterStateProvider().connect();
        assertZKStateProvider(client).getZkStateReader().registerDocCollectionWatcher(collection, watcher);
    }
}