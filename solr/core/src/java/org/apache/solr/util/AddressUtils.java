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
package org.apache.solr.util;

import java.lang.invoke.MethodHandles;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.UnknownHostException;
import java.util.Enumeration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Simple utilities for working Hostname/IP Addresses */
public final class AddressUtils {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  // normalize host removing any url scheme.
  // input can be null, host, or url_prefix://host
  public static String getHostToAdvertise() {
    String hostaddress;
    try {
      hostaddress = InetAddress.getLocalHost().getHostAddress();
    } catch (UnknownHostException e) {
      hostaddress =
          InetAddress.getLoopbackAddress()
              .getHostAddress(); // cannot resolve system hostname, fall through
    }
    // Re-get the IP again for "127.0.0.1", the other case we trust the hosts
    // file is right.
    if ("127.0.0.1".equals(hostaddress)) {
      try {
        var netInterfaces = NetworkInterface.getNetworkInterfaces();
        while (netInterfaces.hasMoreElements()) {
          NetworkInterface ni = netInterfaces.nextElement();
          Enumeration<InetAddress> ips = ni.getInetAddresses();
          while (ips.hasMoreElements()) {
            InetAddress ip = ips.nextElement();
            if (ip.isSiteLocalAddress()) {
              hostaddress = ip.getHostAddress();
            }
          }
        }
      } catch (Exception e) {
        log.error("Error while looking for a better host name than 127.0.0.1", e);
      }
    }
    return hostaddress;
  }
}
