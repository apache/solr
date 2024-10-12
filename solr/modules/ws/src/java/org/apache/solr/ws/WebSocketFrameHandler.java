package org.apache.solr.ws;


/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

import java.util.HashMap;
import java.util.Map;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import org.apache.solr.core.CoreContainer;

/**
 * Echoes uppercase content of text frames.
 */
public class WebSocketFrameHandler extends SimpleChannelInboundHandler<WebSocketFrame> {

  final CoreContainer coreContainer;

  public WebSocketFrameHandler(CoreContainer coreContainer) {
    this.coreContainer = coreContainer;
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, WebSocketFrame frame) throws Exception {
    // ping and pong frames already handled

    if (frame instanceof TextWebSocketFrame) {
      // Send the uppercase string back.
      String request = ((TextWebSocketFrame) frame).text();
      ObjectMapper mapper = new ObjectMapper();
      Map<String,Object> response;
      try {
        Map<String, Object> parsedRequest =
            mapper.readValue(request, new TypeReference<HashMap<String, Object>>() {});
        response = dispatch(parsedRequest);

      } catch ( Exception e ) {
        response = parseFailed(request);
      }
      ctx.channel().writeAndFlush(new TextWebSocketFrame(mapper.writeValueAsString(response)));
    } else {
      String message = "unsupported frame type: " + frame.getClass().getName();
      throw new UnsupportedOperationException(message);
    }
  }

  private Map<String, Object> parseFailed(String request) {
    Map<String, Object> response = new HashMap<>();
    response.put("status", "ERROR: unable to parse request:" + request);
    return response;
  }

  private Map<String, Object> dispatch(Map<String, Object> parsedRequest) {
    // expecting:
    //  {
    //    action: <QUERY|INDEX|ADMIN>
    //    target: {
    //      type: <REPLICA|SHARD|COLLECTION|NODE>   (more later?)
    //      name: <name by which object can be looked up>
    //    }
    //    payload: { <object specific to action, i.e. a query, a document etc }
    //  }
    String action = (String) parsedRequest.get("action");
    switch (action) {
      case "QUERY":
        // Validate target
        // Perform query on local replica or dispatch to node with replica
      case "INDEX":
        // Validate target
        // Perform update (add document to local replica or dispatch to node with replica)
      case "ADMIN":
        // todo: think longer about how to handle admin stuff.
        return unsupported();
      default:
        return unknownAction(action);
    }
  }

  private Map<String, Object> unknownAction(String action) {
    Map<String, Object> response = new HashMap<>();
    response.put("status", "ERROR: "+action+"is not a recognized action");
    return response;
  }

  private Map<String, Object> unsupported() {
    Map<String, Object> response = new HashMap<>();
    response.put("status", "ERROR: this action is not yet supported");
    return response;
  }

  @Override
  public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
    if (evt instanceof WebSocketServerProtocolHandler.HandshakeComplete) {
      //Channel upgrade to websocket, remove WebSocketIndexPageHandler.
      ctx.pipeline().remove(WebSocketIndexPageHandler.class);
    } else {
      super.userEventTriggered(ctx, evt);
    }
  }
}
