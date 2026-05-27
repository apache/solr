/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * BATS test program: opens a SocketChannel to 192.0.2.1:443 (TEST-NET-1, RFC 5737 — guaranteed
 * non-routable). The address is not in the trusted-hosts set and port 443 is not in the default
 * policy. SocketChannelInterceptor fires before any TCP I/O, so the test completes instantly.
 * Expected: SecurityException thrown by SocketChannelInterceptor in enforce mode.
 */
public class NetworkViolation {
  public static void main(String[] args) throws Exception {
    System.out.println("attempting socket connect");
    var addr = new java.net.InetSocketAddress("192.0.2.1", 443);
    try (var ch = java.nio.channels.SocketChannel.open()) {
      ch.connect(addr);
    }
    System.out.println("connect succeeded -- agent did NOT block");
  }
}
