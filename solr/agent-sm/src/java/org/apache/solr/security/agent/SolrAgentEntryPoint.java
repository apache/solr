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
import java.net.Socket;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Path;
import java.nio.file.spi.FileSystemProvider;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.implementation.Implementation;
import net.bytebuddy.matcher.ElementMatchers;

/**
 * Java agent entry point. Invoked by the JVM before the application main class when the agent JAR
 * is specified via {@code -javaagent:}. Loads the policy, initializes {@link AgentPolicy}, and
 * installs ByteBuddy interceptors. The agent JAR is on {@code Boot-Class-Path} so interceptor
 * classes are visible to the bootstrap classloader; SLF4J is excluded from the fat JAR to avoid
 * poisoning the binding before Log4j2 initializes.
 */
public final class SolrAgentEntryPoint {

  private SolrAgentEntryPoint() {}

  public static void premain(String agentArgs, Instrumentation inst) {
    bootAgent(inst);
  }

  /** Called by the JVM when the agent is attached dynamically; delegates to {@link #premain}. */
  public static void agentmain(String agentArgs, Instrumentation inst) {
    premain(agentArgs, inst);
  }

  // ---------------------------------------------------------------------------
  // Internal bootstrap logic
  // ---------------------------------------------------------------------------

  @SuppressForbidden(
      reason =
          "System.err is the only safe output during premain; System.exit(1) is required to halt "
              + "the JVM on fatal policy-load failure in enforce mode.")
  private static void bootAgent(Instrumentation inst) {
    Path defaultPolicyPath = resolveDefaultPolicyPath();

    AgentPolicy policy = null;
    try {
      PolicyLoader loader = new PolicyLoader();
      policy = loader.load(defaultPolicyPath);
    } catch (Exception e) {
      String modeStr = PolicyPropertyExpander.getPropertyOrEnv("solr.security.agent.mode");
      if (modeStr == null) modeStr = "warn";
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

    AgentPolicy.initialize(policy);

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

  private static final String[] FILE_INTERCEPTED_METHODS = {
    "write",
    "createFile",
    "createDirectories",
    "createLink",
    "copy",
    "move",
    "newByteChannel",
    "delete",
    "deleteIfExists",
    "open"
  };

  private static void installInterceptors(Instrumentation inst) {
    // Context.Disabled + NoOp: required for REDEFINE — ByteBuddy must not add auxiliary types or
    // static initializers when redefining already-loaded JDK classes.
    final ByteBuddy byteBuddy =
        new ByteBuddy().with(Implementation.Context.Disabled.Factory.INSTANCE);

    new AgentBuilder.Default(byteBuddy)
        .with(AgentBuilder.InitializationStrategy.NoOp.INSTANCE)
        .with(AgentBuilder.RedefinitionStrategy.REDEFINITION)
        .with(AgentBuilder.TypeStrategy.Default.REDEFINE)
        // Custom ignore filter: allows bootstrap JDK classes (java.lang, java.nio, …) to be
        // instrumented; excludes ByteBuddy's own classes to prevent circular instrumentation.
        .ignore(ElementMatchers.isSynthetic().or(ElementMatchers.nameStartsWith("net.bytebuddy.")))
        // Intercept FileSystemProvider subclasses, java.nio.file.Files, and FileChannel subtypes.
        .type(
            ElementMatchers.isSubTypeOf(FileSystemProvider.class)
                .or(ElementMatchers.named("java.nio.file.Files"))
                .or(ElementMatchers.isSubTypeOf(FileChannel.class)))
        .transform(
            (builder, type, classLoader, module, domain) ->
                builder.visit(
                    Advice.to(FileInterceptor.class)
                        .on(
                            ElementMatchers.namedOneOf(FILE_INTERCEPTED_METHODS)
                                .and(ElementMatchers.not(ElementMatchers.isAbstract())))))
        // Intercept SocketChannel / Socket outbound connections → SocketChannelInterceptor
        .type(
            ElementMatchers.isSubTypeOf(SocketChannel.class)
                .or(ElementMatchers.isSubTypeOf(Socket.class)))
        .transform(
            (builder, type, classLoader, module, domain) ->
                builder.visit(
                    Advice.to(SocketChannelInterceptor.class)
                        .on(
                            ElementMatchers.named("connect")
                                .and(ElementMatchers.not(ElementMatchers.isAbstract())))))
        // Intercept System.exit(int) → SystemExitInterceptor
        .type(ElementMatchers.is(System.class))
        .transform(
            (builder, type, classLoader, module, domain) ->
                builder.visit(
                    Advice.to(SystemExitInterceptor.class).on(ElementMatchers.named("exit"))))
        // halt and exec together: avoids two separate redefinitions of Runtime
        .type(ElementMatchers.is(Runtime.class))
        .transform(
            (builder, type, classLoader, module, domain) ->
                builder
                    .visit(
                        Advice.to(RuntimeHaltInterceptor.class).on(ElementMatchers.named("halt")))
                    .visit(
                        Advice.to(ProcessExecInterceptor.class).on(ElementMatchers.named("exec"))))
        // Intercept ProcessBuilder.start() → ProcessExecInterceptor
        .type(ElementMatchers.is(ProcessBuilder.class))
        .transform(
            (builder, type, classLoader, module, domain) ->
                builder.visit(
                    Advice.to(ProcessExecInterceptor.class).on(ElementMatchers.named("start"))))
        .installOn(inst);
  }

  /** Resolves {@code server/etc/agent-security.policy} from {@code solr.install.dir} or {@code jetty.home}. */
  private static Path resolveDefaultPolicyPath() {
    String installDir = System.getProperty("solr.install.dir");
    if (installDir != null && !installDir.isBlank()) {
      return Path.of(installDir, "server", "etc", "agent-security.policy");
    }
    String jettyHome = System.getProperty("jetty.home", System.getProperty("server.dir", "."));
    return Path.of(jettyHome, "etc", "agent-security.policy");
  }
}
