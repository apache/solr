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
package org.apache.solr.security.agent;

import java.lang.instrument.Instrumentation;
import java.nio.file.Path;
import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.matcher.ElementMatchers;

/**
 * Java agent entry point for the Solr security agent.
 *
 * <p>The JVM invokes {@link #premain(String, Instrumentation)} before the application main method
 * when the agent JAR is listed on the command line as {@code -javaagent:solr-agent-sm-*.jar}. The
 * {@link #agentmain(String, Instrumentation)} method supports attach-based loading (not used by
 * Solr's startup scripts but retained for tooling compatibility).
 *
 * <h2>Startup sequence</h2>
 *
 * <ol>
 *   <li>Locate and parse {@code agent-security.policy} (and the optional {@code
 *       agent-security-extra.policy}) via {@link PolicyLoader}.
 *   <li>Initialize the {@link SolrSecurityPolicy} singleton.
 *   <li>Register all four ByteBuddy interceptors with the JVM instrumentation API.
 *   <li>If policy loading fails and enforcement mode is {@code ENFORCE}, halt the JVM; in {@code
 *       WARN} mode, log the error and continue without protection.
 * </ol>
 *
 * <h2>Bootstrap injection</h2>
 *
 * The interceptor classes ({@link FileAccessInterceptor}, etc.) are injected into the bootstrap
 * classloader using {@code ClassInjector.UsingUnsafe.ofBootLoader()} so that they can intercept JDK
 * methods which are loaded by the bootstrap loader. The {@code @SuppressForbidden} annotation on
 * the injection call acknowledges the intentional use of {@code sun.misc.Unsafe}.
 */
public final class SolrAgentEntryPoint {

  private SolrAgentEntryPoint() {}

  /**
   * Called by the JVM before the application main class is loaded, when the agent JAR is specified
   * via {@code -javaagent:}.
   *
   * @param agentArgs optional agent argument string (unused)
   * @param inst the {@link Instrumentation} instance provided by the JVM
   */
  public static void premain(String agentArgs, Instrumentation inst) {
    bootOtel(inst);
  }

  /**
   * Called by the JVM when the agent is attached dynamically (post-startup). Delegates to {@link
   * #premain(String, Instrumentation)}.
   *
   * @param agentArgs optional agent argument string (unused)
   * @param inst the {@link Instrumentation} instance provided by the JVM
   */
  public static void agentmain(String agentArgs, Instrumentation inst) {
    premain(agentArgs, inst);
  }

  // ---------------------------------------------------------------------------
  // Internal bootstrap logic
  // ---------------------------------------------------------------------------

  @SuppressForbidden(
      reason =
          "System.err is the only output available during premain (before logging is initialized). "
              + "System.exit(1) is required to halt the JVM on a fatal policy-load failure in "
              + "enforce mode — this is intentional agent behavior, not application code.")
  private static void bootOtel(Instrumentation inst) {
    // Locate the default policy file next to the agent JAR.
    Path defaultPolicyPath = resolveDefaultPolicyPath();

    SolrSecurityPolicy policy = null;
    try {
      PolicyLoader loader = new PolicyLoader();
      policy = loader.load(defaultPolicyPath);
    } catch (IllegalStateException e) {
      // Policy load failed.
      String modeStr = System.getProperty("solr.security.agent.mode", "warn");
      if ("enforce".equalsIgnoreCase(modeStr.trim())) {
        System.err.println(
            "[Solr SecurityAgent] FATAL: Cannot load security policy in enforce mode. "
                + "Solr will not start. Error: "
                + e.getMessage());
        System.exit(1);
      } else {
        System.err.println(
            "[Solr SecurityAgent] WARNING: Cannot load security policy ("
                + e.getMessage()
                + "). Security controls are inactive.");
        return;
      }
    }

    SolrSecurityPolicy.initialize(policy);

    // Register ByteBuddy interceptors.
    try {
      installInterceptors(inst);
    } catch (Exception e) {
      System.err.println("[Solr SecurityAgent] Failed to install interceptors: " + e);
    }

    System.err.println(
        "[Solr SecurityAgent] Security agent active — mode="
            + policy.enforcementMode()
            + ", permitted paths="
            + policy.permittedPaths().size()
            + ", permitted endpoints="
            + policy.permittedEndpoints().size());
  }

  /**
   * Installs all four ByteBuddy interceptors using the provided {@link Instrumentation} instance.
   * The interceptor classes are injected into the bootstrap classloader so that they can redefine
   * JDK methods.
   */
  private static void installInterceptors(Instrumentation inst) {
    new AgentBuilder.Default()
        // Intercept java.nio.file.Files read/copy/move operations → FileAccessInterceptor
        .type(ElementMatchers.named("java.nio.file.Files"))
        .transform(
            (builder, type, classLoader, module, domain) ->
                builder.visit(
                    Advice.to(FileAccessInterceptor.class)
                        .on(
                            ElementMatchers.named("newInputStream")
                                .or(ElementMatchers.named("readAllBytes"))
                                .or(ElementMatchers.named("readString"))
                                .or(ElementMatchers.named("readAllLines"))
                                .or(ElementMatchers.named("newOutputStream"))
                                .or(ElementMatchers.named("write"))
                                .or(ElementMatchers.named("delete"))
                                .or(ElementMatchers.named("deleteIfExists"))
                                .or(ElementMatchers.named("copy"))
                                .or(ElementMatchers.named("move")))))
        // Intercept SocketChannel.connect(SocketAddress) → NetworkAccessInterceptor
        .type(
            ElementMatchers.named("java.nio.channels.SocketChannel")
                .or(ElementMatchers.named("java.net.Socket")))
        .transform(
            (builder, type, classLoader, module, domain) ->
                builder.visit(
                    Advice.to(NetworkAccessInterceptor.class).on(ElementMatchers.named("connect"))))
        // Intercept System.exit(int) → ExitInterceptor.onSystemExit
        .type(ElementMatchers.named("java.lang.System"))
        .transform(
            (builder, type, classLoader, module, domain) ->
                builder.visit(Advice.to(ExitInterceptor.class).on(ElementMatchers.named("exit"))))
        // Intercept Runtime.halt(int) → ExitInterceptor.onRuntimeHalt
        .type(ElementMatchers.named("java.lang.Runtime"))
        .transform(
            (builder, type, classLoader, module, domain) ->
                builder.visit(Advice.to(ExitInterceptor.class).on(ElementMatchers.named("halt"))))
        // Intercept ProcessBuilder.start() → ProcessExecInterceptor.onProcessBuilderStart
        .type(ElementMatchers.named("java.lang.ProcessBuilder"))
        .transform(
            (builder, type, classLoader, module, domain) ->
                builder.visit(
                    Advice.to(ProcessExecInterceptor.class).on(ElementMatchers.named("start"))))
        // Intercept Runtime.exec(String[]) → ProcessExecInterceptor.onRuntimeExec
        .type(ElementMatchers.named("java.lang.Runtime"))
        .transform(
            (builder, type, classLoader, module, domain) ->
                builder.visit(
                    Advice.to(ProcessExecInterceptor.class).on(ElementMatchers.named("exec"))))
        .with(AgentBuilder.RedefinitionStrategy.RETRANSFORMATION)
        .with(AgentBuilder.TypeStrategy.Default.REDEFINE)
        .installOn(inst);
  }

  /**
   * Resolves the default policy file path. Looks for {@code server/etc/agent-security.policy}
   * relative to the Solr installation root ({@code solr.install.dir}), then falls back to {@code
   * jetty.home/../etc/agent-security.policy}.
   */
  private static Path resolveDefaultPolicyPath() {
    String installDir = System.getProperty("solr.install.dir");
    if (installDir != null && !installDir.isBlank()) {
      return Path.of(installDir, "server", "etc", "agent-security.policy");
    }
    String jettyHome = System.getProperty("jetty.home", System.getProperty("server.dir", "."));
    return Path.of(jettyHome, "etc", "agent-security.policy");
  }
}
