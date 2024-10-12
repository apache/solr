package org.apache.solr.ws;

import java.lang.invoke.MethodHandles;
import java.security.cert.CertificateException;
import javax.net.ssl.SSLException;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslContext;
import org.apache.solr.common.util.EnvUtils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.ModuleLifecycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StartWebSocket implements ModuleLifecycle {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  static final boolean SSL = EnvUtils.getPropertyAsBool("solr.ws.ssl", false) != null;

  private final CoreContainer coreContainer;
  private EventLoopGroup bossGroup;
  private EventLoopGroup workerGroup;

  public StartWebSocket(CoreContainer coreContainer) {
    this.coreContainer = coreContainer;
  }

  @Override
  public CoreContainer getCoreContainer() {
    return coreContainer;
  }

  @Override
  public void start(CoreContainer cc) {
    log.info("Starting WebSocket bootstrap");
    final SslContext sslCtx;
    try {
      sslCtx = ServerUtil.buildSslContext();
    } catch (CertificateException | SSLException e) {
      throw new RuntimeException(e);
    }
    log.info("WebSocket Using SSL context: {}", sslCtx);
    bossGroup = new NioEventLoopGroup(1);
    workerGroup = new NioEventLoopGroup();
    ServerBootstrap b = new ServerBootstrap();
    b.group(bossGroup, workerGroup)
        .channel(NioServerSocketChannel.class)
        .handler(new LoggingHandler(LogLevel.INFO)) // todo: replace with solr logging
        .childHandler(new WebSocketServerInitializer(sslCtx, coreContainer));
    try {
      int port = coreContainer.getNodeConfig().getCloudConfig().getSolrHostPort() + 1000;
      log.info("binding port: {}", port);
      Channel ch = b.bind(port).sync().channel();
      log.info("Open your web browser and navigate to " +
          (SSL? "https" : "http") + "://127.0.0.1:" + port + '/');
      ch.closeFuture().sync();
    } catch (InterruptedException e) {
      log.error("Error starting WebSocket", e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void shutdown(CoreContainer cc) {
    bossGroup.shutdownGracefully();
    workerGroup.shutdownGracefully();
  }
}
