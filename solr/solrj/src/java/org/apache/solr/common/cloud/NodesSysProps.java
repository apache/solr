package org.apache.solr.common.cloud;

import java.util.Collection;
import java.util.Map;

public interface NodesSysProps {
    Map<String, Object> getSysProps(String node, Collection<String> tags);
}
