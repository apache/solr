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
package org.apache.solr.handler.admin;

import static org.apache.solr.common.params.CommonParams.NAME;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.PlatformManagedObject;
import java.lang.management.RuntimeMXBean;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.nio.file.Path;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.lucene.util.Version;
import org.apache.solr.client.api.model.CoreInfoResponse;
import org.apache.solr.client.api.model.NodeSystemResponse;
import org.apache.solr.client.api.util.SolrVersion;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.EnvUtils;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.NodeConfig;
import org.apache.solr.core.SolrCore;
import org.apache.solr.metrics.GpuMetricsProvider;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.security.AuthorizationPlugin;
import org.apache.solr.security.PKIAuthenticationPlugin;
import org.apache.solr.security.RuleBasedAuthorizationPluginBase;
import org.apache.solr.util.RTimer;
import org.apache.solr.util.stats.MetricUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Used by GetNodeSystemInfo, and indirectly by SystemInfoHandler */
public class SystemInfoProvider {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private SolrQueryRequest req;
  private SolrParams params;
  private CoreContainer cc;

  // on some platforms, resolving canonical hostname can cause the thread
  // to block for several seconds if nameservices aren't available
  // so resolve this once per provider instance
  // (ie: not static, so core reload will refresh)
  private String hostname;

  private static final long ONE_KB = 1024;
  private static final long ONE_MB = ONE_KB * ONE_KB;
  private static final long ONE_GB = ONE_KB * ONE_MB;

  /**
   * Undocumented expert level system property to prevent doing a reverse lookup of our hostname.
   * This property will be logged as a suggested workaround if any problems are noticed when doing
   * reverse lookup.
   *
   * <p>TODO: should we refactor this (and the associated logic) into a helper method for any other
   * places where DNS is used?
   *
   * @see #initHostname
   */
  private static final String REVERSE_DNS_OF_LOCALHOST_SYSPROP =
      "solr.admin.handler.systeminfo.dns.reverse.lookup.enabled";

  /**
   * Local cache for BeanInfo instances that are created to scan for system metrics. List of
   * properties is not supposed to change for the JVM lifespan, so we can keep already create
   * BeanInfo instance for future calls.
   */
  private static final ConcurrentMap<Class<?>, BeanInfo> beanInfos = new ConcurrentHashMap<>();

  public SystemInfoProvider(SolrQueryRequest request) {
    req = request;
    params = request.getParams();
    cc = request.getCoreContainer();
    initHostname();
  }

  /** Fill-out the provided response with all system info. */
  public NodeSystemResponse getNodeSystemInfo(NodeSystemResponse response) {
    NodeSystemResponse.NodeSystemInfo info = new NodeSystemResponse.NodeSystemInfo();

    if (cc != null) {
      info.solrHome = cc.getSolrHome().toString();
      info.coreRoot = cc.getCoreRootDirectory().toString();
    }

    boolean solrCloudMode = cc != null && cc.isZooKeeperAware();
    info.mode = solrCloudMode ? "solrcloud" : "std";
    if (solrCloudMode) {
      info.zkHost = cc.getZkController().getZkServerAddress();
      info.node = cc.getZkController().getNodeName();
    }

    info.lucene = getLuceneInfo();

    info.jvm = getJvmInfo();

    info.security = getSecurityInfo();
    info.system = getSystemInfo();
    info.gpu = getGpuInfo();

    SolrEnvironment env =
        SolrEnvironment.getFromSyspropOrClusterprop(
            solrCloudMode ? cc.getZkController().zkStateReader : null);
    if (env.isDefined()) {
      info.environment = env.getCode();
      if (env.getLabel() != null) {
        info.environmentLabel = env.getLabel();
      }
      if (env.getColor() != null) {
        info.environmentColor = env.getColor();
      }
    }
    response.nodeInfo = info;
    return response;
  }

  /** Get core Info for V1 */
  public static SimpleOrderedMap<Object> getCoreInfo(SolrCore core, IndexSchema schema) {
    SimpleOrderedMap<Object> info = new SimpleOrderedMap<>();

    info.add("schema", schema != null ? schema.getSchemaName() : "no schema!");

    // Now
    info.add("now", new Date());

    // Start Time
    info.add("start", core.getStartTimeStamp());

    // Solr Home
    SimpleOrderedMap<Object> dirs = new SimpleOrderedMap<>();
    dirs.add("cwd", Path.of(System.getProperty("user.dir")).toAbsolutePath().toString());
    dirs.add("instance", core.getInstancePath().toString());
    try {
      dirs.add("data", core.getDirectoryFactory().normalize(core.getDataDir()));
    } catch (IOException e) {
      log.warn("Problem getting the normalized data directory path", e);
    }
    dirs.add("dirimpl", core.getDirectoryFactory().getClass().getName());
    try {
      dirs.add("index", core.getDirectoryFactory().normalize(core.getIndexDir()));
    } catch (IOException e) {
      log.warn("Problem getting the normalized index directory path", e);
    }
    info.add("directory", dirs);
    return info;
  }

  /** Get core info for V2 */
  public CoreInfoResponse getCoreInfo(String coreName, CoreInfoResponse info) {

    SolrCore core = req.getCore();
    log.info("Request SolrCore: {}", core.getName());
    if (req.getCoreContainer() != null
        && req.getCoreContainer().getAllCoreNames().contains(coreName)) {
      core = req.getCoreContainer().getCore(coreName);
      log.info("Requested SolrCore: {}", core.getName());
    }

    if (core == null) return info;

    IndexSchema schema = req.getSchema();

    info.schema = schema != null ? schema.getSchemaName() : "no schema!";
    info.host = hostname;
    info.now = new Date();
    info.start = core.getStartTimeStamp();

    // Solr Home
    CoreInfoResponse.Directory dirs = new CoreInfoResponse.Directory();
    dirs.cwd = Path.of(System.getProperty("user.dir")).toAbsolutePath().toString();
    dirs.instance = core.getInstancePath().toString();
    try {
      dirs.data = core.getDirectoryFactory().normalize(core.getDataDir());
    } catch (IOException e) {
      log.warn("Problem getting the normalized data directory path", e);
      dirs.data = "N/A";
    }
    dirs.dirimpl = core.getDirectoryFactory().getClass().getName();
    try {
      dirs.index = core.getDirectoryFactory().normalize(core.getIndexDir());
    } catch (IOException e) {
      log.warn("Problem getting the normalized index directory path", e);
      dirs.index = "N/A";
    }
    info.directory = dirs;
    return info;
  }

  /** Get system info */
  public Map<String, String> getSystemInfo() {
    Map<String, String> info = new HashMap<>();

    OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();
    info.put(NAME, os.getName()); // add at least this one

    // add remaining ones dynamically using Java Beans API
    // also those from JVM implementation-specific classes
    forEachGetterValue(
        os,
        MetricUtils.OS_MXBEAN_CLASSES,
        (name, value) -> {
          if (info.get(name) == null) {
            info.put(name, String.valueOf(value));
          }
        });

    return info;
  }

  /** Get JVM Info - including memory info */
  public NodeSystemResponse.JVM getJvmInfo() {
    NodeSystemResponse.JVM jvm = new NodeSystemResponse.JVM();

    final String javaVersion = System.getProperty("java.specification.version", "unknown");
    final String javaVendor = System.getProperty("java.specification.vendor", "unknown");
    final String javaName = System.getProperty("java.specification.name", "unknown");
    final String jreVersion = System.getProperty("java.version", "unknown");
    final String jreVendor = System.getProperty("java.vendor", "unknown");
    final String vmVersion = System.getProperty("java.vm.version", "unknown");
    final String vmVendor = System.getProperty("java.vm.vendor", "unknown");
    final String vmName = System.getProperty("java.vm.name", "unknown");

    // Summary Info
    jvm.version = jreVersion + " " + vmVersion;
    jvm.name = jreVendor + " " + vmName;

    // details
    NodeSystemResponse.Vendor spec = new NodeSystemResponse.Vendor();
    spec.vendor = javaVendor;
    spec.name = javaName;
    spec.version = javaVersion;
    jvm.spec = spec;

    NodeSystemResponse.Vendor jre = new NodeSystemResponse.Vendor();
    jre.vendor = jreVendor;
    jre.version = jreVersion;
    jvm.jre = jre;

    NodeSystemResponse.Vendor vm = new NodeSystemResponse.Vendor();
    vm.vendor = vmVendor;
    vm.name = vmName;
    vm.version = vmVersion;
    jvm.vm = vm;

    Runtime runtime = Runtime.getRuntime();
    jvm.processors = runtime.availableProcessors();

    // not thread safe, but could be thread local
    DecimalFormat df = new DecimalFormat("#.#", DecimalFormatSymbols.getInstance(Locale.ROOT));

    NodeSystemResponse.JvmMemory mem = new NodeSystemResponse.JvmMemory();
    NodeSystemResponse.JvmMemoryRaw raw = new NodeSystemResponse.JvmMemoryRaw();
    long free = runtime.freeMemory();
    long max = runtime.maxMemory();
    long total = runtime.totalMemory();
    long used = total - free;
    double percentUsed = ((double) (used) / (double) max) * 100;
    raw.free = free;
    mem.free = humanReadableUnits(free, df);
    raw.total = total;
    mem.total = humanReadableUnits(total, df);
    raw.max = max;
    mem.max = humanReadableUnits(max, df);
    raw.used = used;
    mem.used = humanReadableUnits(used, df) + " (%" + df.format(percentUsed) + ")";
    raw.usedPercent = percentUsed;

    mem.raw = raw;
    jvm.memory = mem;

    // JMX properties
    NodeSystemResponse.JvmJmx jmx = new NodeSystemResponse.JvmJmx();
    try {
      RuntimeMXBean mx = ManagementFactory.getRuntimeMXBean();
      if (mx.isBootClassPathSupported()) {
        jmx.classpath = mx.getBootClassPath();
      }
      jmx.classpath = mx.getClassPath();

      // the input arguments passed to the Java virtual machine
      // which does not include the arguments to the main method.
      NodeConfig nodeConfig = cc != null ? cc.getNodeConfig() : null;
      jmx.commandLineArgs = getInputArgumentsRedacted(nodeConfig, mx);

      jmx.startTime = new Date(mx.getStartTime());
      jmx.upTimeMS = mx.getUptime();

    } catch (Exception e) {
      log.warn("Error getting JMX properties", e);
    }
    jvm.jmx = jmx;
    return jvm;
  }

  /** Get Security Info */
  public NodeSystemResponse.Security getSecurityInfo() {
    NodeSystemResponse.Security info = new NodeSystemResponse.Security();

    if (cc != null) {
      if (cc.getAuthenticationPlugin() != null) {
        info.authenticationPlugin = cc.getAuthenticationPlugin().getName();
      }
      if (cc.getAuthorizationPlugin() != null) {
        info.authorizationPlugin = cc.getAuthorizationPlugin().getClass().getName();
      }
    }

    if (req.getUserPrincipal() != null
        && req.getUserPrincipal() != PKIAuthenticationPlugin.CLUSTER_MEMBER_NODE) {
      // User principal
      info.username = req.getUserPrincipal().getName();

      // Mapped roles for this principal
      // @SuppressWarnings("resource")
      AuthorizationPlugin auth = cc == null ? null : cc.getAuthorizationPlugin();
      if (auth instanceof RuleBasedAuthorizationPluginBase rbap) {
        Set<String> roles = rbap.getUserRoles(req.getUserPrincipal());
        info.roles = roles;
        if (roles == null) {
          info.permissions = Set.of();
        } else {
          info.permissions =
              rbap.getPermissionNamesForRoles(
                  Stream.concat(roles.stream(), Stream.of("*", null)).collect(Collectors.toSet()));
        }
      }
    }

    if (cc != null && cc.getZkController() != null) {
      String urlScheme =
          cc.getZkController().zkStateReader.getClusterProperty(ZkStateReader.URL_SCHEME, "http");
      info.tls = ZkStateReader.HTTPS.equals(urlScheme);
    }

    return info;
  }

  /** Get Lucene and Solr versions */
  public NodeSystemResponse.Lucene getLuceneInfo() {
    NodeSystemResponse.Lucene info = new NodeSystemResponse.Lucene();

    Package p = SolrCore.class.getPackage();
    String specVersion = p.getSpecificationVersion();
    String implVersion = p.getImplementationVersion();
    // non-null mostly for testing
    info.solrSpecVersion = specVersion == null ? SolrVersion.LATEST_STRING : specVersion;
    info.solrImplVersion =
        implVersion == null ? SolrVersion.LATEST.getPrereleaseVersion() : implVersion;

    info.luceneSpecVersion = Version.LATEST.toString();
    info.luceneImplVersion = Version.getPackageImplementationVersion();

    return info;
  }

  /** Get GPU info */
  public NodeSystemResponse.GPU getGpuInfo() {
    NodeSystemResponse.GPU gpuInfo = new NodeSystemResponse.GPU();
    gpuInfo.available = false; // set below if available

    try {
      GpuMetricsProvider provider = cc.getGpuMetricsProvider();

      if (provider == null) {
        return gpuInfo;
      }

      long gpuCount = provider.getGpuCount();
      if (gpuCount > 0) {
        gpuInfo.available = true;
        gpuInfo.count = gpuCount;

        long gpuMemoryTotal = provider.getGpuMemoryTotal();
        long gpuMemoryUsed = provider.getGpuMemoryUsed();
        long gpuMemoryFree = provider.getGpuMemoryFree();

        if (gpuMemoryTotal > 0) {
          NodeSystemResponse.MemoryRaw memory = new NodeSystemResponse.MemoryRaw();
          memory.total = gpuMemoryTotal;
          memory.used = gpuMemoryUsed;
          memory.free = gpuMemoryFree;
          gpuInfo.memory = memory;
        }

        var devices = provider.getGpuDevices();
        if (devices != null && devices.size() > 0) {
          gpuInfo.devices = devices;
        }
      }

    } catch (Exception e) {
      log.warn("Failed to get GPU information", e);
    }

    return gpuInfo;
  }

  /** Return good default units based on byte size. */
  private String humanReadableUnits(long bytes, DecimalFormat df) {
    String newSizeAndUnits;

    if (bytes / ONE_GB > 0) {
      newSizeAndUnits = String.valueOf(df.format((float) bytes / ONE_GB)) + " GB";
    } else if (bytes / ONE_MB > 0) {
      newSizeAndUnits = String.valueOf(df.format((float) bytes / ONE_MB)) + " MB";
    } else if (bytes / ONE_KB > 0) {
      newSizeAndUnits = String.valueOf(df.format((float) bytes / ONE_KB)) + " KB";
    } else {
      newSizeAndUnits = String.valueOf(bytes) + " bytes";
    }

    return newSizeAndUnits;
  }

  private List<String> getInputArgumentsRedacted(NodeConfig nodeConfig, RuntimeMXBean mx) {
    List<String> list = new ArrayList<>();
    for (String arg : mx.getInputArguments()) {
      if (arg.startsWith("-D")
          && arg.contains("=")
          && (nodeConfig != null
              && nodeConfig.isSysPropHidden(arg.substring(2, arg.indexOf('='))))) {
        list.add(
            String.format(
                Locale.ROOT,
                "%s=%s",
                arg.substring(0, arg.indexOf('=')),
                NodeConfig.REDACTED_SYS_PROP_VALUE));
      } else {
        list.add(arg);
      }
    }
    return list;
  }

  /**
   * Iterates over properties of the given MXBean and invokes the provided consumer with each
   * property name and its current value.
   *
   * @param obj an instance of MXBean
   * @param interfaces interfaces that it may implement. Each interface will be tried in turn, and
   *     only if it exists and if it contains unique properties then they will be added as metrics.
   * @param consumer consumer for each property name and value
   * @param <T> formal type
   */
  private <T extends PlatformManagedObject> void forEachGetterValue(
      T obj, String[] interfaces, BiConsumer<String, Object> consumer) {
    for (String clazz : interfaces) {
      try {
        final Class<? extends PlatformManagedObject> intf =
            Class.forName(clazz).asSubclass(PlatformManagedObject.class);
        forEachGetterValue(obj, intf, consumer);
      } catch (ClassNotFoundException e) {
        // ignore
      }
    }
  }

  /**
   * Iterates over properties of the given MXBean and invokes the provided consumer with each
   * property name and its current value.
   *
   * @param obj an instance of MXBean
   * @param intf MXBean interface, one of {@link PlatformManagedObject}-s
   * @param consumer consumer for each property name and value
   * @param <T> formal type
   */
  // protected & static : ref. test in NodeSystemInfoProviderTest
  protected static <T extends PlatformManagedObject> void forEachGetterValue(
      T obj, Class<? extends T> intf, BiConsumer<String, Object> consumer) {
    if (intf.isInstance(obj)) {
      BeanInfo beanInfo =
          beanInfos.computeIfAbsent(
              intf,
              clazz -> {
                try {
                  return Introspector.getBeanInfo(
                      clazz, clazz.getSuperclass(), Introspector.IGNORE_ALL_BEANINFO);

                } catch (IntrospectionException e) {
                  log.warn("Unable to fetch properties of MXBean {}", obj.getClass().getName());
                  return null;
                }
              });

      // if BeanInfo retrieval failed, return early
      if (beanInfo == null) {
        return;
      }
      for (final PropertyDescriptor desc : beanInfo.getPropertyDescriptors()) {
        try {
          Method readMethod = desc.getReadMethod();
          if (readMethod == null) {
            continue; // skip properties without a read method
          }

          final String name = desc.getName();
          Object value = readMethod.invoke(obj);
          consumer.accept(name, value);
        } catch (Exception e) {
          // didn't work, skip it...
        }
      }
    }
  }

  private void initHostname() {
    if (!EnvUtils.getPropertyAsBool(REVERSE_DNS_OF_LOCALHOST_SYSPROP, true)) {
      log.info(
          "Resolving canonical hostname for local host prevented due to '{}' sysprop",
          REVERSE_DNS_OF_LOCALHOST_SYSPROP);
      hostname = null;
      return;
    }

    RTimer timer = new RTimer();
    try {
      InetAddress addr = InetAddress.getLocalHost();
      hostname = addr.getCanonicalHostName();
    } catch (Exception e) {
      log.warn(
          "Unable to resolve canonical hostname for local host, possible DNS misconfiguration. Set the '{}' sysprop to false on startup to prevent future lookups if DNS can not be fixed.",
          REVERSE_DNS_OF_LOCALHOST_SYSPROP,
          e);
      hostname = null;
      return;
    } finally {
      timer.stop();

      if (15000D < timer.getTime()) {
        String readableTime = String.format(Locale.ROOT, "%.3f", (timer.getTime() / 1000));
        log.warn(
            "Resolving canonical hostname for local host took {} seconds, possible DNS misconfiguration. Set the '{}' sysprop to false on startup to prevent future lookups if DNS can not be fixed.",
            readableTime,
            REVERSE_DNS_OF_LOCALHOST_SYSPROP);
      }
    }
  }
}
