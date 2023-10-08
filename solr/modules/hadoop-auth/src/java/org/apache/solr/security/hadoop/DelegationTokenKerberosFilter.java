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
package org.apache.solr.security.hadoop;

import static org.apache.hadoop.security.token.delegation.ZKDelegationTokenSecretManager.ZK_DTSM_ZNODE_WORKING_PATH;
import static org.apache.hadoop.security.token.delegation.ZKDelegationTokenSecretManager.ZK_DTSM_ZNODE_WORKING_PATH_DEAFULT;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.server.AuthenticationHandler;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticationFilter;
import org.apache.hadoop.security.token.delegation.web.HttpUserGroupInformation;
import org.apache.solr.common.cloud.SecurityAwareZkACLProvider;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkACLProvider;
import org.apache.solr.common.cloud.ZkCredentialsProvider;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

/**
 * This is an authentication filter based on Hadoop's {@link DelegationTokenAuthenticationFilter}.
 * The Kerberos plugin can be configured to use delegation tokens, which allow an application to
 * reuse the authentication of an end-user or another application.
 */
public class DelegationTokenKerberosFilter extends DelegationTokenAuthenticationFilter {
  private ExecutorService curatorSafeServiceExecutor;
  private CuratorFramework curatorFramework;

  @Override
  public void init(FilterConfig conf) throws ServletException {
    if (conf != null && "zookeeper".equals(conf.getInitParameter("signer.secret.provider"))) {
      SolrZkClient zkClient =
          (SolrZkClient)
              conf.getServletContext().getAttribute(KerberosPlugin.DELEGATION_TOKEN_ZK_CLIENT);
      try {
        conf.getServletContext()
            .setAttribute(
                "signer.secret.provider.zookeeper.curator.client",
                getCuratorClientInternal(conf, zkClient));
      } catch (InterruptedException | KeeperException e) {
        throw new ServletException(e);
      }
    }
    super.init(conf);
  }

  /**
   * Return the ProxyUser Configuration. FilterConfig properties beginning with
   * "solr.impersonator.user.name" will be added to the configuration.
   */
  @Override
  protected Configuration getProxyuserConfiguration(FilterConfig filterConf) {
    Configuration conf = new Configuration(false);

    Enumeration<String> names = filterConf.getInitParameterNames();
    while (names.hasMoreElements()) {
      String name = names.nextElement();
      if (name.startsWith(KerberosPlugin.IMPERSONATOR_PREFIX)) {
        String value = filterConf.getInitParameter(name);
        conf.set(
            PROXYUSER_PREFIX + "." + name.substring(KerberosPlugin.IMPERSONATOR_PREFIX.length()),
            value);
        conf.set(name, value);
      }
    }
    return conf;
  }

  @Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain filterChain)
      throws IOException, ServletException {
    // include Impersonator User Name in case someone (e.g. logger) wants it
    FilterChain filterChainWrapper =
        new FilterChain() {
          @Override
          public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse)
              throws IOException, ServletException {
            HttpServletRequest httpRequest = (HttpServletRequest) servletRequest;

            UserGroupInformation ugi = HttpUserGroupInformation.get();
            if (ugi != null
                && ugi.getAuthenticationMethod()
                    == UserGroupInformation.AuthenticationMethod.PROXY) {
              UserGroupInformation realUserUgi = ugi.getRealUser();
              if (realUserUgi != null) {
                httpRequest.setAttribute(
                    KerberosPlugin.IMPERSONATOR_USER_NAME, realUserUgi.getShortUserName());
              }
            }
            filterChain.doFilter(servletRequest, servletResponse);
          }
        };

    super.doFilter(request, response, filterChainWrapper);
  }

  @Override
  public void destroy() {
    super.destroy();
    if (curatorFramework != null) {
      curatorFramework.close();
      curatorFramework = null;
    }
    if (curatorSafeServiceExecutor != null) {
      ExecutorUtil.shutdownNowAndAwaitTermination(curatorSafeServiceExecutor);
      curatorSafeServiceExecutor = null;
    }
  }

  @Override
  protected void initializeAuthHandler(String authHandlerClassName, FilterConfig filterConfig)
      throws ServletException {
    // set the internal authentication handler in order to record whether the request should
    // continue
    super.initializeAuthHandler(authHandlerClassName, filterConfig);
    AuthenticationHandler authHandler = getAuthenticationHandler();
    super.initializeAuthHandler(
        RequestContinuesRecorderAuthenticationHandler.class.getName(), filterConfig);
    RequestContinuesRecorderAuthenticationHandler newAuthHandler =
        (RequestContinuesRecorderAuthenticationHandler) getAuthenticationHandler();
    newAuthHandler.setAuthHandler(authHandler);
  }

  private CuratorFramework getCuratorClientInternal(FilterConfig conf, SolrZkClient zkClient)
      throws KeeperException, InterruptedException {
    // There is a race condition where the znodeWorking path used by ZKDelegationTokenSecretManager
    // can be created by multiple nodes, but Hadoop doesn't handle this well. This explicitly
    // creates it up front and handles if the znode already exists. This relates to HADOOP-18452
    // but didn't solve the underlying issue of the race condition.

    // If namespace parents are implicitly created, they won't have ACLs.
    // So, let's explicitly create them.
    CuratorFramework curatorFramework = getCuratorClient(zkClient);
    CuratorFramework nullNsFw = curatorFramework.usingNamespace(null);
    try {
      String znodeWorkingPath =
          '/'
              + Objects.requireNonNullElse(
                  conf.getInitParameter(ZK_DTSM_ZNODE_WORKING_PATH),
                  ZK_DTSM_ZNODE_WORKING_PATH_DEAFULT)
              + "/ZKDTSMRoot";
      nullNsFw.create().creatingParentContainersIfNeeded().forPath(znodeWorkingPath);
    } catch (Exception ignore) {
    }

    return curatorFramework;
  }

  protected CuratorFramework getCuratorClient(SolrZkClient zkClient)
      throws InterruptedException, KeeperException {
    // Create /security znode upfront. Without this, the curator framework creates this directory
    // path
    // without the appropriate ACL configuration. This issue is possibly related to HADOOP-11973
    try {
      zkClient.makePath(
          SecurityAwareZkACLProvider.SECURITY_ZNODE_PATH, CreateMode.PERSISTENT);
    } catch (KeeperException.NodeExistsException ex) {
      // ignore?
    }

    return zkClient.getCuratorFramework().usingNamespace(zkClient.getAbsolutePath(SecurityAwareZkACLProvider.SECURITY_ZNODE_PATH));
  }
}
