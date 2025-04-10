package org.apache.solr.handler.component;

/**
 * Wrapping a HttpShardHandlerFactory to avoid many instances of HttpShardHandlerFactory created by
 * each SolrCore
 */
public class WrappedHttpShardHandlerFactory extends ShardHandlerFactory {
  private ShardHandlerFactory handlerFactory;

  void setHandlerFactory(ShardHandlerFactory handlerFactory) {
    this.handlerFactory = handlerFactory;
  }

  @Override
  public ShardHandler getShardHandler() {
    return handlerFactory.getShardHandler();
  }

  @Override
  public void close() {}
}
