package org.apache.solr.cloud;

import org.apache.solr.client.solrj.impl.BaseCloudSolrClient;
import org.apache.solr.client.solrj.impl.ZkClientClusterStateProvider;

import java.io.Serializable;

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
}