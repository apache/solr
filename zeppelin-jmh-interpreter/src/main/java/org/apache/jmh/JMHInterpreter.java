/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.jmh;

import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.exec.PumpStreamHandler;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.solr.client.solrj.impl.BinaryRequestWriter;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.JavaBinCodec;
import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterException;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.InterpreterResult.Code;
import org.apache.zeppelin.interpreter.ZeppelinContext;
import org.apache.zeppelin.interpreter.thrift.InterpreterCompletion;
import org.apache.zeppelin.scheduler.Scheduler;
import org.apache.zeppelin.scheduler.SchedulerFactory;
import org.eclipse.jetty.io.RuntimeIOException;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.jetbrains.annotations.NotNull;
import org.eclipse.jetty.server.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.apache.commons.io.FileUtils.*;

// gc jcmd 999 VM.log output="file=gc.log" output_options="filecount=5,filesize=10m" what="gc=debug" decorators="time,level"

/**
 * JMH benchmark module interpreter for Zeppelin.
 */
public class JMHInterpreter extends Interpreter {

  private static final Logger log = LoggerFactory.getLogger(JMHInterpreter.class);

  private static final String TIMEOUT_PROPERTY = "jmh.command.timeout";

  private static final String DEFAULT_TIMEOUT = "60000";
  private static final String DEFAULT_CHECK_INTERVAL = "5000";
  private static final String DIRECTORY_USER_HOME = "shell.working.directory.user.home";
  public static final String[] SYS_TRACE_HEADERS = {"epoch", "usr", "sys", "idl", "wai", "stl", "1m", "5m", "15m", "run", "blk", "new", "int", "csw", "read",
      "writ", "recv", "send", "read", "writ", "in", "out", "used", "free", "buff", "cach", "files", "inodes", "lis", "act", "syn", "tim", "clo"};
  private static final boolean STD_TABLE = true;
  public static final String MAP_STORE = "map-store";
  public static final File RESOURCE_SERVE_DIR = new File(new File(System.getProperty("user.home"), "jmh"), "public");
  public static final String TMP_JMH_HEAP_DUMP = "/tmp/jmh-heap-dump";
  public static final String PROFILE_RESULTS = "jmh-profile-results";
  private static final AtomicInteger ID_CNT = new AtomicInteger();

  private static final Pattern COMPLETION = Pattern.compile("Run progress: (\\d+\\.\\d+)% complete");
  public static final String SYSTRACE_2_CSV = "systrace-2.csv";
  public static final String SYSTRACE = "systrace";
  public static final String SYSTRACE_CSV = "systrace.csv";
  public static final String HOSTS = "hosts";
  public static final String SPACER = "        ;\n";
  public static final String LARGE_SPACE = "        });\n";
  public static final String RESULT_SET = "ResultSet";
  public static final String PROFILE_DIR = "$PROFILE_DIR";
  public static final String LOCAL = "local";
  public static final String ARTIFACT_CHAFF = " artifactChaff:";

  private static String DSTAT_CSV_SCRIPT = " success=true\n" + "rm -f $SYSTRACE_CSV\ntouch $SYSTRACE_CSV\n"
      + "dstat -Tclpydnrgm --fs --tcp --noheaders --noupdate --nocolor --output $SYSTRACE_CSV 5 &\n"
      + "dstatPid=$!\n" + "START=$(date +%s.%N)\n" + "$cmd\n"
      + "success=$?\n" + "if [ $success -ne 0 ]; then\n" + " success=false\n" + "fi\n" + "\n" + "END=$(date +%s.%N)\n"
      + "DIFF=$(echo ${END} - ${START} | bc)\n" + "DIFF=$(echo ${DIFF}/1 | bc )\n" + "kill -s TERM ${dstatPid}" + "\n" + "wait ${dstatPid}\n"
      + "wait\n" + "\n" + "tail -n +8 $SYSTRACE_CSV > $SYSTRACE_CSV.tmp && mv $SYSTRACE_CSV.tmp $SYSTRACE_CSV";


  private ConcurrentHashMap<String, DefaultExecutor> executorMap;

  private ConcurrentHashMap<String,Semaphore> semaphores;

  private ConcurrentHashMap<String,StringBuilder> outputs;

  private ConcurrentHashMap<String, InterpreterContext> contextMap;

  private ConcurrentHashMap<String, Integer> currentBenchmark;

  private final ScheduledExecutorService shellOutputCheckExecutor =
      Executors.newSingleThreadScheduledExecutor();

  // TODO: remove - interesting but unessary - everything as folders/files accessable by jetty
  ChronicleMap<CharSequence, byte[]> artifactChaff;
  private Server server;
  static {
    try {
      if (Files.exists(Paths.get("/home/markmiller/Sync"))) {
        Files.writeString(Paths.get("/home/markmiller/Sync/hacklog.log"), "hacklog init", StandardOpenOption.CREATE);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public JMHInterpreter(Properties property) {
    super(property);
  }

  @Override
  public List<InterpreterCompletion> completion(String buf, int cursor,
      InterpreterContext interpreterContext) throws InterpreterException {

    log.info("completion {}, {}, {}", buf, cursor, "");

    if (cursor < 5) {
      return Collections.singletonList(new InterpreterCompletion("cmd=", "cmd=", "jmh command"));
    }

    return Collections.emptyList();
  }


  @Override
  public void open() {
    long timeoutThreshold = Long.parseLong(getProperty(TIMEOUT_PROPERTY, DEFAULT_TIMEOUT));
    long timeoutCheckInterval = Long.parseLong(DEFAULT_CHECK_INTERVAL);
    log.info("Command timeout property: {}", timeoutThreshold);


    executorMap = new ConcurrentHashMap<>();
    currentBenchmark = new ConcurrentHashMap<>();
    contextMap = new ConcurrentHashMap<>();
    semaphores = new ConcurrentHashMap<>();
    outputs = new ConcurrentHashMap<>();

    server = new Server();
    ServerConnector connector = new ServerConnector(server);
    connector.setPort(8191);
    server.setConnectors(new Connector[] {connector});
    ResourceHandler resourceHandler = new ResourceHandler();
    File base = RESOURCE_SERVE_DIR;
    base.mkdirs();
    resourceHandler.setResourceBase(base.getAbsolutePath());
  //  server.setHandler(resourceHandler);
  //  HandlerCollection container = new HandlerCollection();
  //  container.addHandler(resourceHandler);
  //  ServletContextHandler servletHandler = new ServletContextHandler(container, "/");
 //   ServletContext servletContext = new ServletContext();
    ServletContextHandler servletHandler = new ServletContextHandler();
    servletHandler.getServletContext().setAttribute("artifactChaff", artifactChaff);
    servletHandler.getServletContext().setAttribute("contextMap", contextMap);
    servletHandler.addServlet(StatusServlet.class, "/status");
    servletHandler.setHandler(resourceHandler);
    server.setHandler(servletHandler);


  //  servletHandler.setHandler(resourceHandler);

    try {
      server.start();

      artifactChaff = ChronicleMapBuilder
          .of(CharSequence.class, byte[].class)
          .name("jmh-artifact-chaff")
          .entries(5_000_000)
          .averageKey("2GD28CD9S")
          .averageValueSize(1024)
          .createPersistedTo(new File(new File(System.getProperty("user.home"), "jmh"), MAP_STORE));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    shellOutputCheckExecutor.scheduleAtFixedRate(() -> {
      try {
        for (Map.Entry<String, DefaultExecutor> entry : executorMap.entrySet()) {
          String paragraphId = entry.getKey();
          InterpreterContext context = contextMap.get(paragraphId);
          if (context == null) {
            log.warn("No InterpreterContext associated with paragraph: {}", paragraphId);
            continue;
          }

          String output = context.out().toString();

          Matcher m = COMPLETION.matcher(output);
          double complete = 0.0;
          while (m.find()) {
            complete = Double.parseDouble(m.group(1));
          }

          if (context.getStringLocalProperty("workdir2", null) != null) {
            if (currentBenchmark.get(context.getParagraphId()) == 1) {
              complete = complete / 2;
            } else if (currentBenchmark.get(context.getParagraphId()) == 2) {
              complete = complete / 2 + 50.0;
            }
          }

          int progress = (int) Math.round(complete);
          if (progress == 0) {
            progress = 1;
          }
          if (progress > context.getIntLocalProperty("progress", 0)) {
            context.getLocalProperties().put("progress", Integer.toString(progress));
            context.setProgress(progress);
          }
          if (output.length() > 1024) {
            context.out.clear(false);
            context.out.write(output.substring(1024));
            StringBuilder saveOutput = outputs.computeIfAbsent(context.getParagraphId(), s -> new StringBuilder(64));
            saveOutput.append(output);
          }

//          if ((System.currentTimeMillis() - context.out.getLastWriteTimestamp()) >
//              timeoutThreshold) {
//            LOGGER.info("No output for paragraph {} for the last {} milli-seconds, so kill it",
//                paragraphId, timeoutThreshold);
//            executor.getWatchdog().destroyProcess();
//          }
        }
      } catch (Exception e) {
        log.error("Error when checking shell command timeout", e);
      }
    }, timeoutCheckInterval, timeoutCheckInterval, TimeUnit.MILLISECONDS);
  }

  @Override
  public void close() {
    try {
      for (String executorKey : executorMap.keySet()) {
        DefaultExecutor executor = executorMap.remove(executorKey);
        if (executor != null) {
          try {
            executor.getWatchdog().destroyProcess();
          } catch (Exception e) {
            log.error("error destroying executor for paragraphId: " + executorKey, e);
          }
        }
      }

      if (shellOutputCheckExecutor != null) {
        shellOutputCheckExecutor.shutdown();
        try {
          shellOutputCheckExecutor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
           Thread.currentThread().interrupt();
        }
      }
    } finally {
      IOUtils.closeQuietly(artifactChaff);
      try {
        server.stop();
      } catch (Exception e) {
        log.warn("", e);
      }
    }
  }

  @Override public InterpreterResult interpret(String st, InterpreterContext context) throws InterpreterException {

    try {
      InterpreterContext.set(context);
      ZeppelinContext z = getZeppelinContext();
      if (z != null) {
        z.setGui(context.getGui());
        z.setNoteGui(context.getNoteGui());
        z.setInterpreterContext(context);
      }
      boolean interpolate = isInterpolate() || Boolean.parseBoolean(context.getLocalProperties().getOrDefault("interpolate", "false"));
      //    if (interpolate) {
      //      st = super.interpolate(st, context.getResourcePool());
      //    }
      return internalInterpret(st, context);
    } finally {
      outputs.remove(context.getParagraphId());
    }
  }


  protected boolean isInterpolate() {
    return Boolean.parseBoolean(getProperty("jmh.interpolation", "true"));
  }


  public ZeppelinContext getZeppelinContext() {
    return null;
  }

  public InterpreterResult internalInterpret(String script, InterpreterContext context) {
    log.info("Run jmh script '{}' properties='{}'", script, properties);

    if (script.isBlank()) {
      return new InterpreterResult(Code.KEEP_PREVIOUS_RESULT);
    }

    String ssh = context.getStringLocalProperty("ssh", null);
    hackLog("internalInterpret key=" + (ssh == null ? LOCAL : ssh) + " semaphores=" + semaphores);
    Semaphore semaphore = semaphores.computeIfAbsent(ssh == null ? LOCAL : ssh, s -> new Semaphore(1));
    try {
      semaphore.acquire();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      log.error("Interrupted while waiting for semaphore", e);
      throw new RuntimeException(e);
    }
    StringBuilder saveOutput = outputs.computeIfAbsent(context.getParagraphId(), s -> new StringBuilder(64));
    try {
     // String sshWorkingDir = null;

      if (contextMap != null) {
        contextMap.put(context.getParagraphId(), context);
      }

      context.out.setEnableTableAppend(true);

      currentBenchmark.put(context.getParagraphId(), 1);


      long startTime = System.currentTimeMillis();

      boolean systrace = context.getBooleanLocalProperty(SYSTRACE, false);
      hackLog("systrace is:" + systrace + " localprops=" + context.getLocalProperties());

      boolean heapdump = context.getBooleanLocalProperty("heapdump", false);
      hackLog("heapdump is:" + heapdump + " localprops=" + context.getLocalProperties());

      String workingDir = getProperty("jmh.workdir", null);
      workingDir = context.getStringLocalProperty("workdir", workingDir);

      log.info("Found working dir '{}'", workingDir);

      String trimCmd = script.trim();
      log.info("Trim command '{}'", trimCmd);
      if (trimCmd.startsWith("cmd=")) return runCmd(ssh, script, context, trimCmd);

      log.info("Script not a cmd '{}'", script);


      String jmhShCmd;

      if (heapdump) {
        jmhShCmd = "./jmh.sh -rf json -jvmArgsAppend -Ddumpheap=" + TMP_JMH_HEAP_DUMP + " ";
      } else {
        jmhShCmd = "./jmh.sh -rf json ";
      }

      if (ssh != null) {
        log.info("running ssh version");
//        sshWorkingDir = workingDir;
//        workingDir = getProperty("jmh.workdir", null);
      }

      String cmpWorkDir = context.getStringLocalProperty("workdir2", null);

      String[] constantDataAndTable;

      boolean profileDir = script.contains(PROFILE_DIR);
      if (profileDir) {
        script = script.replace("\\;", "\\\\;");

        Path profileResults = Paths.get(workingDir, "work", PROFILE_RESULTS);

        if (Files.exists(profileResults)) {
          FileUtils.deleteDirectory(profileResults.toFile());
          Files.createDirectories(profileResults );
        }
      }

      Path profileResultsDir = null;
      if (cmpWorkDir != null) {
        log.info("found cmp work dir of {}",cmpWorkDir);

        if (profileDir) {
          profileResultsDir = Paths.get(cmpWorkDir, "work", PROFILE_RESULTS);
          deleteDirectory(profileResultsDir.toFile());
        }

        List<Map<String,Object>> flatMapsCmp = runCmpBenchmark(script,
            context, ssh, systrace, jmhShCmd, cmpWorkDir);

        if (profileDir) {
          profileResultsDir = Paths.get(workingDir, "work", PROFILE_RESULTS);
          deleteDirectory(profileResultsDir.toFile());
        }
        currentBenchmark.put(context.getParagraphId(), 2);
        String json = runBenchmark((profileDir ? script.replace(PROFILE_DIR, profileResultsDir.toString()) : script),
            context, ssh, workingDir, systrace, jmhShCmd, SYSTRACE_CSV);
        log.info("json=\n{}", json);

        List<Map<String,Object>> flatMaps = JsonMapFlattener.flatMapsFromResult(json);

        flatMaps.forEach(flatMap -> flatMap.put(RESULT_SET, "After"));

//        Set<String> headerKeys = flatMapsCmp.iterator().next().keySet();
//        flatMaps.forEach(fm -> {
//          Iterator<Map.Entry<String,Object>> it = fm.entrySet().iterator();
//          it.forEachRemaining(entry -> {
//            if (!headerKeys.contains(entry.getKey())) {
//              it.remove();
//            }
//          } );
//        });
//
//        Set<String> headerKeys2 = flatMaps.iterator().next().keySet();
//        flatMapsCmp.forEach(fm -> {
//          Iterator<Map.Entry<String,Object>> it = fm.entrySet().iterator();
//          it.forEachRemaining(entry -> {
//            if (!headerKeys2.contains(entry.getKey())) {
//              it.remove();
//            }
//          } );
//        });
//
//        if (flatMaps.size() != flatMapsCmp.size()) {
//          throw new IllegalStateException("flatmaps=\n" + flatMaps + "\n\nflatmapsCmp\n" + flatMapsCmp );
//        }

        flatMapsCmp.addAll(flatMaps);

        if (flatMaps.isEmpty()) {
          throw new IllegalStateException("empty results from second run");
        }
        if (flatMapsCmp.isEmpty()) {
          throw new IllegalStateException("empty results from first run");
        }

        constantDataAndTable = JsonMapFlattener.flatMapsToZeppelinTable(flatMapsCmp);

      } else {
        if (profileDir) {
          profileResultsDir = Paths.get(workingDir, "work", PROFILE_RESULTS);
          deleteDirectory(profileResultsDir.toFile());
        }

        String json = runBenchmark((profileDir ? script.replace(PROFILE_DIR, profileResultsDir.toString()) : script),
            context, ssh, workingDir, systrace, jmhShCmd, SYSTRACE_CSV);
        log.info("json=\n{}", json);

        constantDataAndTable = JsonMapFlattener.resultsToZeppelinTable(json);

        if (systrace) {
          fechAndStoreTraceFile(context, ssh, workingDir, SYSTRACE_CSV, SYSTRACE_2_CSV);
        }
      }

      log.info("do heapdump={}", heapdump);
      List<String> paths = null;
      if (heapdump) {
        InterpreterResult result = new InterpreterResult(Code.SUCCESS);
        paths = JMHUtils.processHeapDumpFolder(new File(TMP_JMH_HEAP_DUMP), RESOURCE_SERVE_DIR,
            Paths.get(context.getNoteId(), String.valueOf(startTime), "heap-reports"));

        int height = context.getIntLocalProperty("height",1200);

        String[] titles = new String[] {"JXray", "JOverflow", "MAT Suspects", "MAT Top Components", "MAT Overview"};
        int titleIndex = 0;
        StringBuilder sb = new StringBuilder();
        for (String path : paths) {
          String iframe = "<iframe src='http://cm4:8191/" + path + "' frameborder=\"0\" scrolling=\"auto\"\n"
              + " style=\"overflow: hidden; height:" + height + "px; \n"
              + " width: 100%;\" height=\"" + height + "\"px\" width=\"100%\"\n"
              + " >Your browser doesn't support iFrames.</iframe>";
          sb.append(showHideDiv( "<a target='_blank' href='http://cm4:8191/" + path + "'>" + titles[titleIndex++] + "</a>", iframe, context)).append("<br/>");
        }
        context.out.clear();
        context.out.write("%html " + sb);
        return result;
      }

      //  System.out.println("zepoutput:" + zeppelinTable);

      context.out.clear();

//      String constDataTable =
//          StringUtils.replace(START_HTML, "$ID", "cnst-tbl-" + context.getParagraphId()).replaceAll("Cmd Output", "Constant Data") + constantDataAndTable[0]
//              + StringUtils.replace(END_HTML, "$ID", "cnst-tbl-" + context.getParagraphId());

      String constDataTable = showHideDiv("Constant Data", constantDataAndTable[0], context);

      //            + '\n' +  "%table " + constantDataAndTable[1]);
      String tsvTable = constantDataAndTable[1];

      CSVFormat format = CSVFormat.DEFAULT.withDelimiter('\t').withRecordSeparator('\n').withIgnoreSurroundingSpaces()
          .withNullString("(NULL)").withTrim().withAutoFlush(true);
      CSVParser records;
      StringReader recordsStringReader = new StringReader(tsvTable);
      records = format.withFirstRecordAsHeader().parse(recordsStringReader);

      log.info("tsvTable: {}", tsvTable);


      if (profileDir) {
        return writeProfileResults(context, startTime, workingDir);
      } else {
        writeTableToContext(context, records, format, constDataTable);
      }

      //        context.out.write(constantDataAndTable[0]);
      //        context.out.write(constantDataAndTable[1]);

      return new InterpreterResult(Code.SUCCESS);

    } catch (ExecuteException e) {
      int exitValue = e.getExitValue();
      log.error("Can not run command: " + script, e);
      shellOutputCheckExecutor.shutdown();
      Code code = Code.ERROR;
      StringBuilder messageBuilder = new StringBuilder();
      if (exitValue == 143) {
        code = Code.INCOMPLETE;
        messageBuilder.append("Paragraph received a SIGTERM\n");
        log.info("The paragraph {} stopped executing: {}", context.getParagraphId(), messageBuilder);
      }
      messageBuilder.append("ExitValue: ").append(exitValue);

     // context.out.clear(true);
      InterpreterResult result = new InterpreterResult(Code.ERROR,saveOutput.toString());
      result.add(messageBuilder + " " + e.getMessage());

      return result;
    } catch (Exception e) {
      log.error("Error running script: " + script, e);
      shellOutputCheckExecutor.shutdown();
     // context.out.clear(true);
      InterpreterResult result = new InterpreterResult(Code.ERROR, saveOutput.toString());
      result.add(e.getMessage());
      return result;
    } finally {
      if (context.getParagraphId() != null) {
        executorMap.remove(context.getParagraphId());
        if (contextMap != null) {
          contextMap.remove(context.getParagraphId());
        }
        if (currentBenchmark != null) {
          currentBenchmark.remove(context.getParagraphId());
        }
      }
      semaphore.release();
    }
  }

  private static AtomicInteger SHOW_HIDE_ID = new AtomicInteger();
  private String showHideDiv(String title, String content, InterpreterContext context) {
    int inc = SHOW_HIDE_ID.incrementAndGet();
    String id = context.getParagraphId() + "-" + inc;
    String startHtml = "<style>\n" + "#coll-" + id + " {\n" + "color: #636578;\n" + "cursor: copy;\n" + "}\n" + "</style>\n" + "<div>\n"
        + "<span id=\"coll-" + id + "\">▲</span><span style='color: #636578;font-size: 14px;'>" + title + "</span>\n" + "<div id=\"hide-" + id + "\" style='display: none;'>";

    String endHtml = "</div>\n" + "</div>\n" + "<script src=\"https://ajax.googleapis.com/ajax/libs/jquery/3.5.1/jquery.min.js\"></script>\n"
        + "<script type=\"text/javascript\">\n" + "$(document).ready(function () {\n" + "$('#coll-" + id + "').click(function (e) {\n"
        + "if ( $('#hide-" + id + "').is(':visible')) {\n" + "$('#hide-" + id + "').hide(\"fast\", \"swing\");\n" + " $('#coll-" + id + "').text('▲');\n" + "} else {\n"
        + "$('#hide-" + id + "').show(\"fast\", \"swing\");\n" + "$('#coll-" + id + "').text('▼');\n" + "}\n" + "});\n" + "});\n" + "</script>";

    return startHtml + content + endHtml;
  }

  @NotNull private InterpreterResult writeProfileResults(InterpreterContext context, long startTime, String workingDir) throws IOException {
    //        InterpreterResult result = new InterpreterResult(Code.SUCCESS, InterpreterResult.Type.HTML, "");
    //        String iframe = "<iframe src='http://cm4:8191/$RESOURCE_PATH' style='border: 0; width: 100%; height:1200px'>Your browser doesn't support iFrames.</iframe>";
    //        StringBuilder html = new StringBuilder();
    //        Path profileResults = Paths.get(workingDir, "work", PROFILE_RESULTS);
    //
    //        Files.list(profileResults).forEach(path -> {
    //          Path dest = Paths.get(RESOURCE_SERVE_DIR.getAbsolutePath(), PROFILE_RESULTS);
    //          try {
    //
    //            if (Files.exists(Paths.get(dest.toString(), path.getFileName().toString()))) {
    //              FileUtils.deleteDirectory(Paths.get(dest.toString(), path.getFileName().toString()).toFile());
    //            }
    //            org.apache.commons.io.FileUtils.moveDirectoryToDirectory(path.toFile(), dest.toFile(), true);
    //          } catch (IOException e) {
    //            throw new RuntimeIOException(e);
    //          }
    //
    //          Pattern extractFlameGraph = Pattern.compile("(<style>.*</script>)", Pattern.MULTILINE | Pattern.DOTALL);
    //          Matcher m;
    //          try {
    //            m = extractFlameGraph.matcher(Files.readString(Paths.get(dest.toString(),path.getFileName().toString(), "flame-cpu-forward.html"), StandardCharsets.UTF_8));
    //          } catch (IOException e) {
    //            throw new IORuntimeException(e);
    //          }
    //          if (!m.find()) {
    //            throw new IllegalStateException("Could not extract html from flamegraph");
    //          }
    //
    //          String extractedHtml = m.group(1);
    //
    //          extractedHtml = extractedHtml
    //              .replaceAll("\\'reverse\\'", "'reverse" + ID_CNT.incrementAndGet() + "'")
    //              .replaceAll("\\'search\\'", "'search" + ID_CNT.incrementAndGet() + "'")
    //              .replaceAll("\\'reset\\'", "'reset" + ID_CNT.incrementAndGet() + "'")
    //              .replaceAll("\\#reset", "#reset" + ID_CNT.get())
    //              .replaceAll("\\'canvas\\'", "'canvas" + ID_CNT.incrementAndGet() + "'")
    //              .replaceAll("hl", "hl" + ID_CNT.incrementAndGet())
    //              .replaceAll("\\'matchval\\'", "'matchval" + ID_CNT.incrementAndGet() + "'")
    //              .replaceAll("\\'match\\'", "'match" + ID_CNT.incrementAndGet() + "'")
    //              .replaceAll("\\#match", "#match" + ID_CNT.get())
    //              .replaceAll("\\'status\\'", "'status" + ID_CNT.incrementAndGet() + "'")
    //              .replaceAll("\\#status", "#status" + ID_CNT.get());
    ////          #hl {position: absolute; display: none; overflow: hidden; white-space: nowrap; pointer-events: none; background-color: #ffffe0; outline: 1px solid #ffc000; height: 15px}
    ////	#hl span {padding: 0 3px 0 3px}
    ////	#status {overflow: hidden; white-space: nowrap}
    ////	#match {overflow: hidden; white-space: nowrap; display: none; float: right; text-align: right}
    ////	#reset
    //
    //          html.append("%html <a target='_blank' href='");
    //          html.append("http://cm4:8191/" + PROFILE_RESULTS + "/" + Paths.get(path.getFileName().toString(), "flame-cpu-forward.html"));
    //          html.append("'>Full Screen</a>");
    //          html.append(extractedHtml + "\n");
    //
    //
    //          result.add(html.toString());
    //
    ////          try {
    ////            context.out.write(html.toString());
    ////          } catch (IOException e) {
    ////            throw new RuntimeIOException(e);
    ////          }
    //        });
    //        return result;

    StringBuilder html = new StringBuilder();

    Path profileResults = Paths.get(workingDir, "work", PROFILE_RESULTS);

    Path dest = Paths.get(RESOURCE_SERVE_DIR.getAbsolutePath(), PROFILE_RESULTS);
    Path outdir = dest.resolve(Paths.get(context.getNoteId())).resolve(String.valueOf(startTime));
    FileUtils.forceMkdir(outdir.toFile());

    try (Stream<Path> filesStream = Files.list(profileResults)) {
      filesStream.forEach(path -> {

        try {

          if (Files.exists(outdir.resolve(path.getFileName()))) {
            FileUtils.deleteDirectory(outdir.resolve(path.getFileName()).toFile());
          }
          // org.apache.commons.io.FileUtils.moveDirectory(path.toFile(), new new File(path.toString() ))
          FileUtils.moveDirectoryToDirectory(path.toFile(), outdir.toFile(), true);
        } catch (IOException e) {
          throw new RuntimeIOException(e);
        }
        String htmlFileName = "flame-cpu-forward.html";
        Path flameHtmlPath = outdir.resolve(path.getFileName()).resolve(htmlFileName);
        if (!Files.exists(flameHtmlPath)) {
          htmlFileName = "flame-alloc-forward.html";
          flameHtmlPath = outdir.resolve(path.getFileName()).resolve(htmlFileName);
        }

        String flameHtml;
        try {
          flameHtml = Files.readString(flameHtmlPath, StandardCharsets.UTF_8);
        } catch (IOException e) {
          throw new IORuntimeException(e);
        }

        Pattern extractFlameGraph = Pattern.compile("<canvas(.*?)</canvas>");
        Matcher m;

        m = extractFlameGraph.matcher(flameHtml);

        if (!m.find()) {
          throw new IllegalStateException("Could not extract html from flamegraph");
        }

        String extractedCanvas = m.group(1);

        Pattern extractHeight = Pattern.compile("height:\\s*?(\\d+)px'");
        Matcher m2;

        m2 = extractHeight.matcher(extractedCanvas);

        if (!m2.find()) {
          throw new IllegalStateException("Could not extract html from flamegraph");
        }
        // int height = Integer.parseInt(m2.group(1)) + 10;

        m2 = extractHeight.matcher(flameHtml);
        if (!m2.find()) {
          throw new IllegalStateException("Could not find height in flamegraph");
        }

        int height = context.getIntLocalProperty("height", 800);

        // flameHtml = m2.replaceFirst("height: " + (height - 185) + "px'");
        // flameHtml = m2.replaceFirst("'");

        flameHtml = flameHtml.replace("font: 12px Verdana", "font: 8px Verdana");

        flameHtml = flameHtml.replace("</body>", "<span id='end'/></body>");
        try {
          Files.writeString(flameHtmlPath, flameHtml, StandardCharsets.UTF_8);
        } catch (IOException e) {
          try {
            String concat = Files.list(outdir).collect(StringBuilder::new, StringBuilder::append, StringBuilder::append).toString();

            throw new IORuntimeException(concat, e);
          } catch (IOException ex) {
            e.addSuppressed(ex);
          }
        }
        //  html.append("<a target='_blank' href='");
        //  html.append("http://cm4:8191/" + PROFILE_RESULTS + "/" + Paths.get(path.getFileName().toString(), "flame-cpu-forward.html"));
        //  html.append("'>Full Screen</a>");
        //          String iframe = "<iframe src='http://cm4:8191/$RESOURCE_PATH' frameborder=\"0\" scrolling=\"yes\"                           \n"
        //              + "                                    style=\"overflow: hidden; height:"  + height + "px; \n"
        //              + "                                                width: 49%; float: left; \" height=\""  + height + "\"px\" width=\"49%\"\n"
        //              + "                                   align=\"left\">Your browser doesn't support iFrames.</iframe>";
        //          html.append(iframe.replaceAll("\\$RESOURCE_PATH", PROFILE_RESULTS + "/" + Paths.get(path.getFileName().toString(), "flame-cpu-forward.html")));
        String iframe = "<iframe src='http://cm4:8191/$RESOURCE_PATH' frameborder=\"0\" scrolling=\"auto\"                           \n" + "                                    style=\"overflow: hidden; height:" + height + "px; \n"
            + "                                                width: 100%;\" height=\"" + height + "\"px\" width=\"100%\"\n" + "                                    >Your browser doesn't support iFrames.</iframe>";
        html.append("<h4>").append("<a target='_blank' href='").append(
            "http://cm4:8191/" + PROFILE_RESULTS + "/" + Paths.get(context.getNoteId(), String.valueOf(startTime), path.getFileName().toString(),
                htmlFileName)).append("'>").append(path.getFileName().toString()).append("</a>").append("</h4>");
        html.append(iframe.replace("$RESOURCE_PATH",
            PROFILE_RESULTS + "/" + Paths.get(context.getNoteId(), String.valueOf(startTime), path.getFileName().toString(), htmlFileName + "#end")));

        //          try {
        //            context.out.write(html.toString());
        //          } catch (IOException e) {
        //            throw new RuntimeIOException(e);
        //          }
      });
    }
    return new InterpreterResult(Code.SUCCESS, InterpreterResult.Type.HTML, html.toString());
  }

  private String runBenchmark(String script, InterpreterContext context, String ssh, String workingDir, boolean systrace, String jmhShCmd, String traceCsvFileName)
      throws IOException {
    CommandLine cmdLine;
    if (ssh != null) {
      log.info("running ssh version");
      cmdLine = CommandLine.parse("ssh " + ssh);
    } else {
      log.info("running local version");
      String shell = "bash -c";
      cmdLine = CommandLine.parse(shell);
    }

    log.info("run {}", script);
    if (systrace) {
      Files.deleteIfExists(Paths.get(workingDir, traceCsvFileName));
      setProperty("lastSysTraceNoteId", context.getNoteId());
      cmdLine.addArgument((ssh != null ? "cd " + workingDir + ";" : "") + DSTAT_CSV_SCRIPT.replace("$SYSTRACE_CSV", traceCsvFileName).replace("$cmd", jmhShCmd + script), false);
    } else {
      cmdLine.addArgument((ssh != null ? "cd " + workingDir + ";" : "") + jmhShCmd + script, false);
    }

    log.info("run benchmark working dir is {}", Paths.get(workingDir).toAbsolutePath());

    Files.deleteIfExists(Paths.get(workingDir, "jmh-result.json"));

    DefaultExecutor executor = runCmds(context, ssh != null ? null : workingDir);

    log.info("execute benchmark with cmdline={}", cmdLine);
    int exitVal = executor.execute(cmdLine);

    log.info("Paragraph {} return with exit value: {}", context.getParagraphId(), exitVal);

    if (exitVal != 0) {
      throw new RuntimeException("Running benchmark returned non 0 exit value: " + exitVal);
    }

    String json = getJson(workingDir, ssh);

    if (systrace) {
      fechAndStoreTraceFile(context, ssh, workingDir, traceCsvFileName, null);
    }
    return json;
  }

  private void fechAndStoreTraceFile(InterpreterContext context, String ssh, String workingDir, String traceCsvFileName, String remove) throws IOException {
    Reader in;

    if (ssh != null) {
      DefaultExecutor getRemoteTraceExec = new DefaultExecutor();
      ByteArrayOutputStream baos = new ByteArrayOutputStream(512);
      getRemoteTraceExec.setStreamHandler(new PumpStreamHandler(baos, baos));
      CommandLine remoteTraceCmdLine = CommandLine.parse("ssh " + ssh);

      remoteTraceCmdLine.addArgument("cat " + workingDir + "/" + traceCsvFileName, false);

      int getRemoteTraceExitValue = getRemoteTraceExec.execute(remoteTraceCmdLine);

      in = new StringReader(baos.toString(StandardCharsets.UTF_8));
    } else {
      in = new FileReader(new File(workingDir, traceCsvFileName));
    }
    // store trace and clear trace for previous paired cmd2
    storeSystrace(ssh, context, in, traceCsvFileName, remove);
  }

  private List<Map<String,Object>> runCmpBenchmark(String script, InterpreterContext context, String ssh, boolean systrace, String jmhShCmd,
      String cmpWorkDir) throws IOException {
    List<Map<String,Object>> flatMaps2 = null;
    Reader in = null;
    try {
      if (contextMap != null) {
        contextMap.put(context.getParagraphId(), context);
      }

      boolean profileDir = script.contains(PROFILE_DIR);
      String cmpJson;
      if (profileDir) {
        cmpJson = runBenchmark(
            script.replace(PROFILE_DIR,
                Paths.get(cmpWorkDir, "work", PROFILE_RESULTS).toString()),
            context,
            ssh,
            cmpWorkDir,
            systrace,
            jmhShCmd,
            SYSTRACE_2_CSV);
      } else {
        cmpJson = runBenchmark(
            script,
            context, ssh, cmpWorkDir, systrace, jmhShCmd, SYSTRACE_2_CSV);
      }

      log.info("cmpJson=\n{}", cmpJson);

      flatMaps2 = JsonMapFlattener.flatMapsFromResult(cmpJson);
      flatMaps2.forEach(flatMap -> {
        flatMap.put(RESULT_SET, "Before");
      });

      if (systrace) {
        String traceName = SYSTRACE_2_CSV;
        if (ssh != null) {
          DefaultExecutor getRemoteTraceExec = new DefaultExecutor();
          getRemoteTraceExec.setWorkingDirectory(new File(cmpWorkDir));
          ByteArrayOutputStream baos = new ByteArrayOutputStream(512);
          getRemoteTraceExec.setStreamHandler(new PumpStreamHandler(baos, baos));
          CommandLine getRemoteTraceCmdLine = CommandLine.parse("ssh " + ssh);

          getRemoteTraceCmdLine.addArgument("cat " + cmpWorkDir + "/" + traceName, false);

          int getRemoteTraceExitVal = getRemoteTraceExec.execute(getRemoteTraceCmdLine);

          in = new StringReader(new String(baos.toByteArray(), StandardCharsets.UTF_8));
        } else {
          in = new FileReader(new File(cmpWorkDir, traceName));
        }

        storeSystrace(ssh, context, in, traceName, null);
      }
    } finally {
      IOUtils.closeQuietly(in);
    }
    return flatMaps2;
  }

  private String getJson(String workingDir, String ssh) throws IOException {
    int exitVal;
    DefaultExecutor executor;
    CommandLine cmdLine;
    String json;
    if (ssh != null) {
      executor = new DefaultExecutor();
      log.info("get json working dir is {}", workingDir);
      executor.setWorkingDirectory(new File(workingDir));
      ByteArrayOutputStream baos = new ByteArrayOutputStream(512);
      executor.setStreamHandler(new PumpStreamHandler(baos, baos));
      cmdLine = CommandLine.parse("ssh " + ssh);

      cmdLine.addArgument("cat " + workingDir + "/jmh-result.json", false);

      exitVal = executor.execute(cmdLine);

      json = baos.toString(StandardCharsets.UTF_8);
    } else {
      json = java.nio.file.Files.readString(java.nio.file.Paths.get(workingDir, "jmh-result.json"));
    }
    return json;
  }

  private void storeSystrace(String ssh, InterpreterContext context, Reader in, String name, String remove) throws IOException {
    // section storeSystrace


    hackLog("store systrace noteid:" + context.getNoteId());

    assert name != null : name;

    String tsv = parseTraceFile1(in);

    log.info("store systrace ssh={}, name={}, remove={}", ssh, name, remove);

    byte[] hostsMapBin = artifactChaff.get(HOSTS);
    Map<String, Map<String,Map<String,String>>> hostsMap = null;
    try {
      if (hostsMapBin == null) {
        hostsMap = new HashMap<>();
      } else {
        //noinspection unchecked
        hostsMap = (Map<String,Map<String,Map<String,String>>>) frmJavaBin(hostsMapBin);
      }
    } catch (Exception e) {
      throw new RuntimeException(artifactChaff.toString() + " map=" + hostsMap, e);
    }
    hostsMap.forEach((k, v) -> {
      if (k == null) throw new IllegalArgumentException("Null Host");
    });

    Map<String,Map<String,String>> hostMap = hostsMap.computeIfAbsent(ssh == null ? LOCAL : ssh, k -> new HashMap<>());

    Map<String,String> systraceMap = hostMap.computeIfAbsent(SYSTRACE, k -> new HashMap<>());

    //  String systrace = (String) systraceMap.get(name);

    systraceMap.put(name, tsv);
    if (remove != null) {
      systraceMap.remove(remove);
    }
    hackLog("systrace host:" + hostMap + " systraceMap=" + systraceMap);

    hostsMap.forEach((k, v) -> {
      if (k == null) throw new IllegalArgumentException("Null Host");
    });

    artifactChaff.put(HOSTS, toJavaBin(hostsMap));

  }

  private void writeTableToContext(InterpreterContext context, CSVParser tsvTable, CSVFormat format, String constDataTable) throws IOException {
    if (STD_TABLE) {

    //  List<String> suppress = List.of("","","","");

      List<String> headers = tsvTable.getHeaderNames();
      List<String> returnHeaders = new ArrayList<>();

      headers.forEach(h -> {
        if (!h.toLowerCase(Locale.ROOT).contains("raw") && !h.toLowerCase(Locale.ROOT).contains("scorepercentile") && !h.toLowerCase(Locale.ROOT)
            .contains("scoreconfidence") && !h.toLowerCase(Locale.ROOT).contains("error")) {
          returnHeaders.add(h);
        }
      });

      StringWriter sw = new StringWriter();
      CSVPrinter printer = new CSVPrinter(sw, format.withHeader(returnHeaders.toArray(new String[0])));

      //.withSkipHeaderRecord(false));
      Iterator<CSVRecord> it = tsvTable.iterator();
      try {
        while (it.hasNext()) {
          CSVRecord rec = it.next();
          List<Object> vals = new ArrayList<>();

          for (String type : returnHeaders) {
            String val;
            if (!rec.isSet(type)) {
              //val = "N/A";
              throw new IllegalStateException("invalid tsv file");
            } else {
              val = rec.get(type);
            }

            vals.add(val);
          }
          printer.printRecord(vals);
        }
      } catch (IllegalArgumentException e) {
        log.error("tsv string looks invalid - headers requested are= {}", returnHeaders, e);
        throw e;
      }

      printer.close(true);

      context.out.write("%html " +constDataTable + "\n%table " + sw.toString());
      return;
    }

    String js;

    //String csvHeaders = tsvTable.substring(0, tsvTable.indexOf('\n')).replaceAll("\t", ",");

    //     String[] headers = csvHeaders.split(",");
    // String dataJs = "data = Object.assign(csvToArray(" + tsvTable + "), ({" + csvHeaders + "}) => ({ops/s: +ops/s})).sort((a) => d3.descending(a.value)), {format: \"%\"})";

    //   log.info("js: {}", dataJs);

    String dataJs2 = "format = x.tickFormat(20, data.format)\ny = d3.scaleBand()\n"
        + "    .domain(d3.range(data.length))\n"
        + "    .rangeRound([margin.top, height - margin.bottom])\n"
        + "    .padding(0.1)\n"
        + "xAxis = g => g\n"
        + "    .attr(\"transform\", `translate(0,${margin.top})`)\n"
        + "    .call(d3.axisTop(x).ticks(width / 80, data.format))\n"
        + "    .call(g => g.select(\".domain\").remove())\n"
        + "yAxis = g => g\n"
        + "    .attr(\"transform\", `translate(${margin.left},0)`)\n"
        + "    .call(d3.axisLeft(y).tickFormat(i => data[i].name).tickSizeOuter(0))\n"
        + "barHeight = 250\n"
        + "height = Math.ceil((data.length + 0.1) * barHeight) + margin.top + margin.bottom\n"
        + "margin = ({top: 30, right: 0, bottom: 10, left: 30})\n";

    //+ "chart = { const svg = d3.create(\"svg\") .attr(\"viewBox\", [0, 0, width, height]); svg.append(\"g\") .attr(\"fill\", \"steelblue\") .selectAll(\"rect\") .data(data) .join(\"rect\") .attr(\"x\", x(0)) .attr(\"y\", (d, i) => y(i)) .attr(\"width\", d => x(d.value) - x(0)) .attr(\"height\", y.bandwidth()); svg.append(\"g\") .attr(\"fill\", \"white\") .attr(\"text-anchor\", \"end\") .attr(\"font-family\", \"sans-serif\") .attr(\"font-size\", 12) .selectAll(\"text\") .data(data) .join(\"text\") .attr(\"x\", d => x(d.value)) .attr(\"y\", (d, i) => y(i) + y.bandwidth() / 2) .attr(\"dy\", \"0.35em\") .attr(\"dx\", -4) .text(d => format(d.value)) .call(text => text.filter(d => x(d.value) - x(0) < 20) // short bars .attr(\"dx\", +4) .attr(\"fill\", \"black\") .attr(\"text-anchor\", \"start\")); svg.append(\"g\") .call(xAxis); svg.append(\"g\") .call(yAxis); return svg.node(); }";
    //            context.out.write("%html <svg width=\"600\" height=\"500\"></svg><script>"
    //                + "\n" + js +   "\n"
    //                + dataJs + "\n"
    //                + dataJs2+ "\n"
    //                + "var svg = d3.select(\"svg\").data(data),\n"
    //                + "        margin = 200,\n"
    //                + "        width = svg.attr(\"width\") - margin,\n"
    //                + "        height = svg.attr(\"height\") - margin;\n"
    //                + "  var xScale = d3.scaleBand().range ([0, width]).padding(0.4),\n" + "        yScale = d3.scaleLinear().range ([height, 0]);\n" + "\n"
    //                + "    var g = svg.append(\"g\")\n"
    //                + "               .attr(\"transform\", \"translate(\" + 100 + \",\" + 100 + \")\");"
    //                + "</script>");


    StringBuilder data = new StringBuilder("var data = [\n"
        + "       {\n"
        + "           key: \"Cumulative Return\",\n"
        + "           values: [");

    //.withSkipHeaderRecord(false));
    List<String> headerNames = tsvTable.getHeaderNames();
    log.info("header: {}", headerNames);
    for (CSVRecord rec : tsvTable) {
      for (String header : headerNames) {
        String val = rec.get(header);
        if (!NumberUtils.isCreatable(val)) {
         // val = "\"" + val + "\"";
          continue;
        }
        data.append("               {\n" + "                   \"label\" : \"").append(header)
            .append("\" ,\n").append("                   \"value\" : ").append(val).append("\n")
            .append("               } ,");
      }
    }

    data.setLength(data.length() - 1);

    data.append("           ]\n" + "       }\n" + "   ];");

    log.info("data: {}", data);
    //
    //        margin: 0px;
    //        padding: 0px;
    //        height: 100%;
    //        width: 100%;

    context.out.write("%html \n"
        + " <script src=\"https://cdnjs.cloudflare.com/ajax/libs/nvd3/1.8.6/nv.d3.min.js\" integrity=\"sha512-ldXL88WIgBA+vAsJu2PepKp3VUvwuyqmXKEbcf8rKeAI56K8GZMb2jfKSm1a36m5AfUzyDp3TIY0iVKY8ciqSg==\" crossorigin=\"anonymous\" referrerpolicy=\"no-referrer\"></script>"
    //    + "<style type=\"text/css\">   @import url(\"https://cdnjs.cloudflare.com/ajax/libs/nvd3/1.8.6/nv.d3.css\"); </style>\n"

        + "<div id=\"chart1\" style='display: block;margin: 0px; padding: 0px;height: 100%;width: 100%;'>\n" + "    <svg style='margin: 10px; padding: 0px;height: 95%;width: 90%;'></svg>\n" + "</div>\n\n"
        + "<script>\n"
        + data + "\n"


//            + "    nv.addGraph(function() {\n" + "        var chart = nv.models.discreteBarChart()\n" + "            .x(function(d) {return d.label})\n"
//            + "            .y(function(d) {return d.value})\n" + "            .staggerLabels(true)\n"
//            + "            .color([\"#4e79a7\",\"#f28e2c\",\"#e15759\",\"#76b7b2\",\"#59a14f\",\"#edc949\",\"#af7aa1\",\"#ff9da7\",\"#9c755f\",\"#bab0ab\"])\n"
//            + "            .showLegend(true)\n"
//
//            + "            .showValues(true)\n" + "            .duration(250);\n"
//            + "\n" + "        d3.select('#chart1 svg')\n" + "            .datum(data)\n" + "            .call(chart);\n" + "\n"
//            + "        nv.utils.windowResize(chart.update);\n" + "        return chart;\n" + "    });"
        + "    var m_data = data.map(function(data, i) {\n"
        + "        return {\n" + "            key: 'Stream' + i,\n"
        + "            values: data\n" + "        };\n" + "    });"
        + "    var chart;\n" + "    nv.addGraph(function() {\n" + "        chart = nv.models.multiBarChart()\n"
        + "            .barColor(d3.scale.category20().range())\n" + "            .duration(300)\n" + "            .margin({bottom: 100, left: 70})\n"
        + "            .rotateLabels(45)\n" + "            .groupSpacing(0.1)\n" + SPACER + "\n"
        + "        chart.reduceXTicks(false).staggerLabels(true);\n" + "\n" + "        chart.xAxis\n"
        + "            .axisLabel(\"ID of Furry Cat Households\")\n" + "            .axisLabelDistance(35)\n" + "            .showMaxMin(false)\n"
        + "            .tickFormat(d3.format(',.6f'))\n" + SPACER + "\n" + "        chart.yAxis\n"
        + "            .axisLabel(\"Change in Furry Cat Population\")\n" + "            .axisLabelDistance(-5)\n"
        + "            .tickFormat(d3.format(',.01f'))\n" + SPACER + "\n" + "        chart.dispatch.on('renderEnd', function(){\n"
        + "            nv.log('Render Complete');\n" + LARGE_SPACE + "\n" + "        d3.select('#chart1 svg')\n"
        + "            .datum(m_data)\n" + "            .call(chart);\n" + "\n" + "        nv.utils.windowResize(chart.update);\n" + "\n"
        + "        chart.dispatch.on('stateChange', function(e) {\n" + "            nv.log('New State:', JSON.stringify(e));\n" + LARGE_SPACE
        + "        chart.state.dispatch.on('change', function(state){\n" + "            nv.log('state', JSON.stringify(state));\n" + LARGE_SPACE
        + "\n"
        + "        return chart;\n" + "    });"


        + "</script>");

    //  data = Object.assign(d3.csvParse(await FileAttachment("alphabet.csv").text(), ({letter, frequency}) => ({name: letter, value: +frequency})).sort((a, b) => d3.descending(a.value, b.value)), {format: "%"})

  }

  @NotNull private InterpreterResult runCmd(String ssh, String cmd, InterpreterContext context, String trimCmd) throws IOException {
    if (trimCmd.startsWith("cmd=systrace")) {
      // section systraceGet
      log.info("cmd is systrace ssh={}", ssh);

      String[] systraceGetParams = trimCmd.substring("cmd=systrace ".length()).split(" ");

      int index = 0;

      try {
        index = systraceGetParams.length < 1 ? 0 : Integer.parseInt(systraceGetParams[0]);
      } catch(NumberFormatException e) {
        // expected
      }

      log.info("artifactChaff={}", artifactChaff);
      Map hostsMap;
      byte[] data = artifactChaff.get(HOSTS);
      try {
        if (data == null) {
          throw new IllegalArgumentException(artifactChaff.toString());
        } else {
          hostsMap = (Map) frmJavaBin(data);
        }
      } catch(Exception e) {
        throw new RuntimeException("note id="+context.getNoteId() + ARTIFACT_CHAFF + artifactChaff.toString(), e);
      }

      Map hostMap = (Map) hostsMap.get(ssh == null ? LOCAL : ssh);

      if (hostMap == null) {
        throw new ResourceNotFoundException("host=" + (ssh == null ? LOCAL : ssh) + " hostsMap:" + hostsMap + ARTIFACT_CHAFF
            + artifactChaff);
      }

      Map systraceMap = (Map) hostMap.get(SYSTRACE);

      if (systraceMap == null) {
        throw new ResourceNotFoundException("key=systrace" + ARTIFACT_CHAFF + artifactChaff);
      }


      Object systraceData = systraceMap.get(SYSTRACE_CSV);
      if (systraceData == null) {
        throw new ResourceNotFoundException("key=systrace.csv" + ARTIFACT_CHAFF + artifactChaff);
      }

      log.info("got systrace data {}", systraceData);


      Object systraceData2 = systraceMap.get(SYSTRACE_2_CSV);
      if (systraceData2 != null) {

        log.info("got systrace data2 {}", systraceData2);
      }

//      Iterable<CSVRecord> records = CSVFormat.DEFAULT
//          .withDelimiter(',')
//          .withRecordSeparator('\n')
//          .withSkipHeaderRecord(true).withHeader(SYS_TRACE_HEADERS).parse(new StringReader((String) systraceData));
      

   //   System.out.println("parsed csv file to ");
      //  records.forEach(i -> System.out.println("rec:" + i));
      List<String> paramsList = Arrays.asList(systraceGetParams);
      List<String> headers = paramsList.subList(index, paramsList.size());

      List<String> actualHeaders = new ArrayList<>();
      if (systraceData2 != null) {
        actualHeaders.add(RESULT_SET);
        actualHeaders.addAll(headers);
      } else {
        actualHeaders = headers;
      }
      StringWriter sw = new StringWriter();
      CSVPrinter printer = new CSVPrinter(sw, CSVFormat.DEFAULT
          .withDelimiter('\t')
          .withRecordSeparator('\n')
          .withHeader(actualHeaders.toArray(new String[0])));

      if (systraceData2 != null) {
        CSVParser records = CSVFormat.DEFAULT.withDelimiter(',').withRecordSeparator('\n').withSkipHeaderRecord(true).withHeader(SYS_TRACE_HEADERS)
            .parse(new StringReader((String) systraceData2));

        //.withSkipHeaderRecord(false));
        for (CSVRecord rec : records) {
          List<Object> vals = new ArrayList<>();

          vals.add("Before");

          for (String type : headers) {
            String val = rec.get(type);
            vals.add(val);
          }
          printer.printRecord(vals);
        }
      }
      CSVParser records = CSVFormat.DEFAULT.withDelimiter(',').withRecordSeparator('\n').withSkipHeaderRecord(true).withHeader(SYS_TRACE_HEADERS)
          .parse(new StringReader((String) systraceData));
      //.withSkipHeaderRecord(false));
      for (CSVRecord rec : records) {
        List<Object> vals = new ArrayList<>();
        if (systraceData2 != null) {
          vals.add("After");
        }
        for (String type : headers) {
          String val;
          try {
            val = rec.get(type);
          } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(type, e);
          }

          vals.add(val);
        }
        printer.printRecord(vals);
      }

      printer.close(true);

      context.out.write("%table " + sw);
      return new InterpreterResult(Code.SUCCESS);
    } if (trimCmd.startsWith("cmd=chart")) {
      String[] systraceGetParams = trimCmd.substring("cmd=chart ".length()).split(" ");


      //context.out.write("%text " + msg);
      String msg = "";
      return new InterpreterResult(Code.SUCCESS, msg);
    } else {
      throw new IllegalArgumentException(cmd + ":" + trimCmd);
    }
  }

  private Object frmJavaBin(byte[] data) throws IOException {
    try (JavaBinCodec jbc = new JavaBinCodec()) {
      Object val = jbc.unmarshal(new ByteArrayInputStream(data));
      log.info("unmarshalled={}", val);
      return val;
    }
  }

  private byte[] toJavaBin(Object object) throws IOException {
    try (final JavaBinCodec jbc = new JavaBinCodec()) {
      BinaryRequestWriter.BAOS baos = new BinaryRequestWriter.BAOS();
      jbc.marshal(object, baos);
      return baos.getbuf();
    }

  }

  private String parseTraceFile1(Reader in) throws IOException {

    // System.out.println("read csv file  " + new File(workingDir, cmdFile + ".csv"));
    // System.out.println("file:  " + Files.readString(Paths.get(workingDir, cmdFile + ".csv")));
    Iterable<CSVRecord> records = CSVFormat.DEFAULT.withDelimiter(',').withRecordSeparator('\n').withSkipHeaderRecord(true)
        .withHeader(SYS_TRACE_HEADERS).parse(in);

    //  System.out.println("parsed csv file to ");
    //  records.forEach(i -> System.out.println("rec:" + i));
    StringWriter sw = new StringWriter();
    CSVPrinter printer = new CSVPrinter(sw, CSVFormat.DEFAULT.withDelimiter(',').withRecordSeparator('\n')
        .withHeader(SYS_TRACE_HEADERS));
    // .withFirstRecordAsHeader()
    //.withSkipHeaderRecord(false));
    for (CSVRecord rec : records) {
      for (String s : rec) {
        printer.print(s);
      }
      printer.println();
    }

    printer.close(true);
    return sw.toString();

  }

  private DefaultExecutor runCmds(InterpreterContext context, String workingDir) {
    DefaultExecutor executor = new DefaultExecutor();

    log.info("working dir is {}", workingDir);
    if (workingDir !=null) {
      executor.setWorkingDirectory(new File(workingDir));
    }
    executor.setStreamHandler(new PumpStreamHandler(context.out, context.out));
    executor.setWatchdog(new ExecuteWatchdog(Long.MAX_VALUE));
    executorMap.put(context.getParagraphId(), executor);
    return executor;
  }

  @Override
  public void cancel(InterpreterContext context) {
    DefaultExecutor executor = executorMap.remove(context.getParagraphId());
    if (executor != null) {
      try {
        executor.getWatchdog().destroyProcess();
      } catch (Exception e){
        log.error("error destroying executor for paragraphId: " + context.getParagraphId(), e);
      }
    }
  }

  @Override
  public FormType getFormType() {
    return FormType.SIMPLE;
  }

  @Override
  public int getProgress(InterpreterContext context) {

    if (context == null || context.out == null) {
      return 0;
    }

    String output = context.out().toString();

    Matcher m = COMPLETION.matcher(output);
    double complete = 1.0;
    while (m.find()) {
      complete = Double.parseDouble(m.group(1));
    }

    return (int) Math.round(complete);
  }

  @Override
  public Scheduler getScheduler() {
    return SchedulerFactory.singleton().createOrGetParallelScheduler(
        JMHInterpreter.class.getName() + this.hashCode(), 10);
  }

  public Map<String, DefaultExecutor> getExecutorMap() {
    return executorMap;
  }

  private static class ResourceNotFoundException extends RuntimeException {
    public ResourceNotFoundException(String msg) {
      super(msg);
    }
  }

  public static void hackLog(String msg) {
//    try {
//      if (Files.exists(Paths.get("/home/markmiller/Sync"))) {
//        Files.writeString(Paths.get("/home/markmiller/Sync/hacklog.log"), msg + '\n', StandardOpenOption.APPEND);
//      }
//    } catch (IOException e) {
//      e.printStackTrace();
//    }
  }
}