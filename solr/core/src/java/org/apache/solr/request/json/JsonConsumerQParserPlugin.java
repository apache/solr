package org.apache.solr.request.json;

import org.apache.solr.search.QParserPlugin;

/**
 * Marker interface for {@link QParserPlugin}s that consume raw JSON query values directly,
 * rather than a stringified local params representation.
 */
public interface JsonConsumerQParserPlugin {}
