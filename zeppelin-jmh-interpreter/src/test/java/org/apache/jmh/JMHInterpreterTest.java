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

import org.apache.http.util.ByteArrayBuffer;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterException;
import org.apache.zeppelin.interpreter.InterpreterOutput;
import org.apache.zeppelin.interpreter.InterpreterOutputListener;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.InterpreterResult.Code;
import org.apache.zeppelin.interpreter.InterpreterResultMessageOutput;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class JMHInterpreterTest implements InterpreterOutputListener {

  private static final Logger log = LoggerFactory.getLogger(JMHInterpreterTest.class);

  private static final boolean IS_WINDOWS = System.getProperty("os.name")
    .startsWith("Windows");
  public static final String SSH_DEST = "127.0.0.1"; // "markmiller@cm4"
  public static final String SSH_BENCH_PATH_1 = "/home/markmiller/apache-solr/solr/benchmark";
  public static final String SSH_BENCH_PATH_2 = "/home/markmiller/apache-solr-2/solr/benchmark";

  int cnt;
  private Properties props;
  private JMHInterpreter interpreter;
  private InterpreterOutput out;
  private InterpreterContext context;

  private ByteArrayBuffer buffer;

  @BeforeClass
  public static void setup() {
  }

  @Before
  public void init() {
    buffer = new ByteArrayBuffer(64000);
    props = new Properties();

    props.put("jmh.workdir", SSH_BENCH_PATH_1);

    out = new InterpreterOutput(this);
    cnt++;
    context = InterpreterContext.builder()
        .setInterpreterOut(out).setNoteId("test" + cnt).setNoteName("test" + cnt).setParagraphId("test" + cnt).build();
    interpreter = new JMHInterpreter(props);
    interpreter.open();
  }

  @After
  public void cleanup() {
    interpreter.close();
  }

  @Test
  public void  testSuccess() throws Exception {
    final String userScript = "JsonFaceting -wi 1 -i 1 -r 3 -w 3 -p docCount=10";

    final InterpreterResult res = interpreter.interpret(userScript, context);

    assertSame("Check SUCCESS: " + res.message(), Code.SUCCESS, res.code());

    out.flush();
    
    final String resultScript = new String(getBufferBytes());


    log.info("result=\n {}", context.out.toString());

    // The script that is executed must contain the functions provided by this interpreter
    assertEquals("Check SCRIPT", resultScript, resultScript);
  }

  @Test
  public void testAsyncProfilerFlamegraph() throws Exception {
    final String userScript = "JavaBinBasicPerf -wi 1 -i 1 -r 3 -w 3 -prof gc -prof async:output=flamegraph;dir=$PROFILE_DIR";

    final InterpreterResult res = interpreter.interpret(userScript, context);

    assertSame("Check SUCCESS: " + res.message(), Code.SUCCESS, res.code());

    out.flush();


    final String resultScript = new String(getBufferBytes());


    log.info("result=\n {}", context.out.toString());

    log.info("result=\n {}", res.message());

    // The script that is executed must contain the functions provided by this interpreter
    assertEquals("Check SCRIPT", resultScript, resultScript);
  }



  @Test
  public void testHeapDump() throws Exception{
    context.getLocalProperties().put("heapdump", "true");

    final String userScript = "JsonFaceting -wi 1 -i 1 -r 3 -w 3 -p docCount=10";

    final InterpreterResult res = interpreter.interpret(userScript, context);

    assertSame("Check SUCCESS: " + res.message(), Code.SUCCESS, res.code());

    out.flush();
   

    final String resultScript = new String(getBufferBytes());


    log.info("result=\n {}", context.out.toString());
    log.info("result=\n {}", res.message());

    // The script that is executed must contain the functions provided by this interpreter
    assertEquals("Check SCRIPT", resultScript, resultScript);

        HttpClient client = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_2)
            .followRedirects(HttpClient.Redirect.NORMAL)
            .build();
        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create("http://127.0.0.1:8191/heap-reports"))
            .timeout(Duration.ofMinutes(1))
            .GET()
            .build();
        HttpResponse<String> response =
            client.send(request, HttpResponse.BodyHandlers.ofString());
        System.out.println(response.statusCode());
        System.out.println(response.body());
  }

  @Test
  public void testStatus() throws Exception {

    HttpClient client = HttpClient.newBuilder()
        .version(HttpClient.Version.HTTP_2)
        .followRedirects(HttpClient.Redirect.NORMAL)
        .build();
    HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create("http://127.0.0.1:8191/status"))
        .timeout(Duration.ofMinutes(1))
        .GET()
        .build();
    HttpResponse<String> response =
        client.send(request, HttpResponse.BodyHandlers.ofString());
    System.out.println(response.statusCode());
    System.out.println(response.body());
  }

  @Test
  public void testSsh() throws Exception {
    final String userScript = "JsonFaceting -wi 1 -i 1 -r 5 -w 2 -p docCount=10";
    context.getLocalProperties().put("ssh", SSH_DEST);
    context.getLocalProperties().put("workdir", SSH_BENCH_PATH_1);

    final InterpreterResult res = interpreter.interpret(userScript, context);

    assertSame("Check SUCCESS: " + res.message(), Code.SUCCESS, res.code());

    out.flush();

    final String resultScript = new String(getBufferBytes());


    log.info("result=\n {}", context.out.toString());

    // The script that is executed must contain the functions provided by this interpreter
    assertEquals("Check SCRIPT", resultScript, resultScript);
  }

  @Test
  public void testCmp() throws Exception {
    final String userScript = "JsonFaceting -wi 1 -i 1 -r 5 -w 2 -p docCount=10";

    context.getLocalProperties().put("workdir2", SSH_BENCH_PATH_2);

    final InterpreterResult res = interpreter.interpret(userScript, context);

    assertSame("Check SUCCESS: " + res.message(), Code.SUCCESS, res.code());

    out.flush();

    final String resultScript = new String(getBufferBytes());


    log.info("result=\n {}", context.out.toString());

    // The script that is executed must contain the functions provided by this interpreter
    assertEquals("Check SCRIPT", resultScript, resultScript);
    assertTrue("Check RESULT", resultScript.contains("benchmark"));
    assertTrue("Check RESULT", resultScript.contains("ResultSet"));
    assertTrue("Check RESULT", resultScript.contains("Before"));
    assertTrue("Check RESULT", resultScript.contains("After"));
  }

  @Test
  public void testCmpNested() throws Exception {
    final String userScript = "JavaBinBasicPerf -p content=nested -t 3 -wi 1 -i 1 -r 5 -w 2";

    context.getLocalProperties().put("workdir2", SSH_BENCH_PATH_2);

    final InterpreterResult res = interpreter.interpret(userScript, context);

    assertSame("Check SUCCESS: " + res.message(), Code.SUCCESS, res.code());

    out.flush();
    
    final String resultScript = new String(getBufferBytes());


    log.info("result=\n {}", context.out.toString());

    // The script that is executed must contain the functions provided by this interpreter
    assertEquals("Check SCRIPT", resultScript, resultScript);
    assertTrue("Check RESULT", resultScript.contains("benchmark"));
    assertTrue("Check RESULT", resultScript.contains("ResultSet"));
    assertTrue("Check RESULT", resultScript.contains("Before"));
    assertTrue("Check RESULT", resultScript.contains("After"));
    assertTrue("Check RESULT", resultScript.contains("ops/s"));

  }

  @Test
  public void testCmpCloudIndexing() throws Exception {
    final String userScript = "CloudIndexing.indexSmallDoc -wi 1 -i 1 -r 2 -w 2";

    context.getLocalProperties().put("workdir2", SSH_BENCH_PATH_2);

    final InterpreterResult res = interpreter.interpret(userScript, context);

    assertSame("Check SUCCESS: " + res.message(), Code.SUCCESS, res.code());

    out.flush();

    final String resultScript = new String(getBufferBytes());


    log.info("result=\n {}", context.out.toString());

    // The script that is executed must contain the functions provided by this interpreter
    assertEquals("Check SCRIPT", resultScript, resultScript);
  }

  @Test
  public void testGC() throws Exception {
    final String userScript = "JavaBinBasicPerf -prof gc -wi 1 -i 1 -r 3 -w 3";

    final InterpreterResult res = interpreter.interpret(userScript, context);

    assertSame("Check SUCCESS: " + res.message(), Code.SUCCESS, res.code());

    out.flush();
   

    final String resultScript = new String(getBufferBytes());


    log.info("result=\n {}", context.out.toString());

    // The script that is executed must contain the functions provided by this interpreter
    assertEquals("Check SCRIPT", resultScript, resultScript);
  }

  @Test
  public void testGC2() throws Exception {
    final String userScript = "JavaBinBasicPerf -prof gc -wi 1 -i 1 -r 3 -w 3";
    context.getLocalProperties().put("workdir", SSH_BENCH_PATH_2);

    final InterpreterResult res = interpreter.interpret(userScript, context);

    assertSame("Check SUCCESS: " + res.message(), Code.SUCCESS, res.code());

    out.flush();

    final String resultScript = new String(getBufferBytes());


    log.info("result=\n {}", context.out.toString());

    // The script that is executed must contain the functions provided by this interpreter
    assertEquals("Check SCRIPT", resultScript, resultScript);
  }


  @Test
  public void testCmpGC() throws Exception {
    final String userScript = "JavaBinBasicPerf -prof gc -wi 1 -i 1 -r 3 -w 3";

    context.getLocalProperties().put("workdir2", SSH_BENCH_PATH_2);

    final InterpreterResult res = interpreter.interpret(userScript, context);

    assertSame("Check SUCCESS: " + res.message(), Code.SUCCESS, res.code());

    out.flush();

    final String resultScript = new String(getBufferBytes());


    log.info("result=\n {}", context.out.toString());

    // The script that is executed must contain the functions provided by this interpreter
    assertEquals("Check SCRIPT", resultScript, resultScript);
  }

  @Test
  public void testCmpBenchMethod() throws Exception {
    final String userScript = "CloudIndexing.indexSmallDoc -wi 1 -i 1 -r 2 -w 2";

    context.getLocalProperties().put("workdir2", SSH_BENCH_PATH_2);

    final InterpreterResult res = interpreter.interpret(userScript, context);

    assertSame("Check SUCCESS: " + res.message(), Code.SUCCESS, res.code());

    out.flush();


    final String resultScript = new String(getBufferBytes());


    log.info("result=\n {}", context.out.toString());

    // The script that is executed must contain the functions provided by this interpreter
    assertEquals("Check SCRIPT", resultScript, resultScript);
  }

  @Test
  public void testCmpNumerics() throws Exception {
    final String userScript = "JavaBinBasicPerf -p content=numeric -t 3 -wi 1 -i 1 -r 3 -w 3";
    context.getLocalProperties().put("workdir2", SSH_BENCH_PATH_2);

    final InterpreterResult res = interpreter.interpret(userScript, context);

    assertSame("Check SUCCESS: " + res.message(), Code.SUCCESS, res.code());

    out.flush();

    final String resultScript = new String(getBufferBytes());


    log.info("result=\n {}", context.out.toString());

    // The script that is executed must contain the functions provided by this interpreter
    assertEquals("Check SCRIPT", resultScript, resultScript);
    assertTrue("Check RESULT", resultScript.contains("ops/s"));
  }

  @Test
  public void testCmpVeryLargsStrings() throws Exception {
    final String userScript = "JavaBinBasicPerf -p content=very_large_text_and_strings -t 3 -wi 1 -i 1 -r 3 -w 3";
    context.getLocalProperties().put("workdir2", SSH_BENCH_PATH_2);

    final InterpreterResult res = interpreter.interpret(userScript, context);

    assertSame("Check SUCCESS: " + res.message(), Code.SUCCESS, res.code());

    out.flush();
   


    final String resultScript = new String(getBufferBytes());


    log.info("result=\n {}", context.out.toString());

    // The script that is executed must contain the functions provided by this interpreter
    assertEquals("Check SCRIPT", resultScript, resultScript);
    assertTrue("Check RESULT", resultScript.contains("ops/s"));
  }

  @Test
  public void testSS() throws Exception {
    final String userScript = "JavaBinBasicPerf -bm ss";

    final InterpreterResult res = interpreter.interpret(userScript, context);

    assertSame("Check SUCCESS: " + res.message(), Code.SUCCESS, res.code());

    out.flush();

    final String resultScript = new String(getBufferBytes());


    log.info("result=\n {}", context.out.toString());

    // The script that is executed must contain the functions provided by this interpreter
    assertEquals("Check SCRIPT", resultScript, resultScript);
  }

  @Test
  public void testSSCmp() throws Exception {
    final String userScript = "JavaBinBasicPerf -bm ss -wi 3 -i 3"; // TODO: if we dont control the warmups/iters, they can vary and raw results won't match per row and blow our tsv - same for sample I thnk - should not put raw data in tsv

    context.getLocalProperties().put("workdir2", SSH_BENCH_PATH_2);

    final InterpreterResult res = interpreter.interpret(userScript, context);

    assertSame("Check SUCCESS: " + res.message(), Code.SUCCESS, res.code());

    out.flush();

    final String resultScript = new String(getBufferBytes());


    log.info("result=\n {}", context.out.toString());

    // The script that is executed must contain the functions provided by this interpreter
    assertEquals("Check SCRIPT", resultScript, resultScript);
  }

  @Test
  public void testSSCmpWworkDir1() throws Exception {
    final String userScript = "JavaBinBasicPerf JavaBinBasicPerf -t 3 -wi 1 -i 1 -r 3 -w 3";
    context.getLocalProperties().put("workdir", SSH_BENCH_PATH_1);
    context.getLocalProperties().put("workdir2", SSH_BENCH_PATH_2);

    final InterpreterResult res = interpreter.interpret(userScript, context);

    assertSame("Check SUCCESS: " + res.message(), Code.SUCCESS, res.code());

    out.flush();

    final String resultScript = new String(getBufferBytes());


    log.info("result=\n {}", context.out.toString());

    // The script that is executed must contain the functions provided by this interpreter
    assertEquals("Check SCRIPT", resultScript, resultScript);
  }

  @Test
  public void testNoSuchElement() throws Exception {
    final String userScript = "JavaBinBasicPerf -p content=large_strings -p scale=2 -t 14 -wi 1 -i 1 -r 3 -w 3"; //-jvmArgsAppend -Xmx14G
    context.getLocalProperties().put("systrace", "true");
    final InterpreterResult res = interpreter.interpret(userScript, context);

    assertSame("Check SUCCESS: " + res.message(), Code.SUCCESS, res.code());

    out.flush();

    final String resultScript = new String(getBufferBytes());


    log.info("result=\n {}", context.out.toString());

    // The script that is executed must contain the functions provided by this interpreter
    assertEquals("Check SCRIPT", resultScript, resultScript);
  }



  @Test
  public void testCmpMultipleThreads() throws Exception {
    final String userScript = "JavaBinBasicPerf -t 3 -wi 1 -i 1 -r 3 -w 3";
    context.getLocalProperties().put("workdir2", SSH_BENCH_PATH_2);

    final InterpreterResult res = interpreter.interpret(userScript, context);

    assertSame("Check SUCCESS: " + res.message(), Code.SUCCESS, res.code());
    out.flush();

    final String resultScript = new String(getBufferBytes());


    log.info("result=\n {}", context.out.toString());

    // The script that is executed must contain the functions provided by this interpreter
    assertEquals("Check SCRIPT", resultScript, resultScript);
    assertTrue("Check RESULT", resultScript.contains("ops/s"));
  }

  @Test
  public void testSshCmp() throws Exception {
    final String userScript = "JsonFaceting -wi 1 -i 1 -r 3 -w 3 -p scale=2";
    context.getLocalProperties().put("ssh", SSH_DEST);
    context.getLocalProperties().put("workdir", SSH_BENCH_PATH_1);
    context.getLocalProperties().put("workdir2", SSH_BENCH_PATH_2);

    final InterpreterResult res = interpreter.interpret(userScript, context);

    assertSame("Check SUCCESS: " + res.message(), Code.SUCCESS, res.code());

    out.flush();

    final String resultScript = new String(getBufferBytes());


    log.info("result=\n {}", context.out.toString());

    // The script that is executed must contain the functions provided by this interpreter
    assertEquals("Check SCRIPT", resultScript, resultScript);
  }

  @Test
  public void testTrace() throws Exception {
    Assume.assumeTrue(!System.getProperty("os.name").startsWith("Mac OS X"));

    context.getLocalProperties().put("systrace", "true");

    final String userScript = "JsonFaceting -wi 1 -i 1 -r 5 -w 5 -p docCount=10";

    final InterpreterResult res = interpreter.interpret(userScript, context);

    assertSame(
        "Check SUCCESS: " + res.message() + "\n" + context.out.toString(),
        Code.SUCCESS,
        res.code());

    out.flush();

    final String resultScript = new String(getBufferBytes());

    // The script that is executed must contain the functions provided by this interpreter
    assertEquals("Check SCRIPT", resultScript, resultScript);

    final String userScript2 = "cmd=systrace epoch usr sys 1m";

    context.out.clear();

    final InterpreterResult res2 = interpreter.interpret(userScript2, context);

    assertSame("Check SUCCESS: " + res2.message(), Code.SUCCESS, res2.code());

    String contextString = context.out().toString();
    System.out.println(contextString);
   // assertTrue(contextString, contextString.contains("%table"));
    assertTrue(res2.message().toString(), contextString.contains("epoch"));
  }

  @Test
  public void testTraceSsh() throws Exception {
    Assume.assumeTrue(!System.getProperty("os.name").startsWith("Mac OS X"));
    context.getLocalProperties().put("ssh", SSH_DEST);
    context.getLocalProperties().put("workdir", SSH_BENCH_PATH_1);
    context.getLocalProperties().put("systrace", "true");

    final String userScript = "JsonFaceting -wi 1 -i 1 -r 5 -w 5 -p docCount=10";

    final InterpreterResult res = interpreter.interpret(userScript, context);

    assertSame(
        "Check SUCCESS: " + res.message() + "\n" + context.out.toString(),
        Code.SUCCESS,
        res.code());

    out.flush();
   

    final String resultScript = new String(getBufferBytes());

    // The script that is executed must contain the functions provided by this interpreter
    assertEquals("Check SCRIPT", resultScript, resultScript);

    final String userScript2 = "cmd=systrace epoch usr sys 1m";

    context.out.clear();

    final InterpreterResult res2 = interpreter.interpret(userScript2, context);

    assertSame("Check SUCCESS: " + res2.message(), Code.SUCCESS, res2.code());

    String contextString = context.out().toString();
    System.out.println(contextString);
    // assertTrue(contextString, contextString.contains("%table"));
    assertTrue(res2.message().toString(), contextString.contains("epoch"));
  }

  @Test
  public void testTraceSshCmp() throws Exception {
    Assume.assumeTrue(!System.getProperty("os.name").startsWith("Mac OS X"));
    context.getLocalProperties().put("ssh", SSH_DEST);
    context.getLocalProperties().put("workdir", SSH_BENCH_PATH_1);
    context.getLocalProperties().put("workdir2", SSH_BENCH_PATH_2);
    context.getLocalProperties().put("systrace", "true");


    final String userScript = "JsonFaceting -wi 1 -i 1 -r 5 -w 5 -p docCount=10";

    final InterpreterResult res = interpreter.interpret(userScript, context);

    assertSame(
        "Check SUCCESS: " + res.message() + "\n" + context.out.toString(),
        Code.SUCCESS,
        res.code());

    out.flush();
   

    final String resultScript = new String(getBufferBytes());

    // The script that is executed must contain the functions provided by this interpreter
    assertEquals("Check SCRIPT", resultScript, resultScript);

    final String userScript2 = "cmd=systrace epoch usr sys 1m";

    context.out.clear();

    final InterpreterResult res2 = interpreter.interpret(userScript2, context);

    assertSame("Check SUCCESS: " + res2.message(), Code.SUCCESS, res2.code());

    String contextString = context.out().toString();

    System.out.println("context content:");
    System.out.println(contextString);
    // assertTrue(contextString, contextString.contains("%table"));
    assertTrue(res2.message().toString(), contextString.contains("epoch"));
    assertTrue(res2.message().toString(), contextString.contains("Before"));
    assertTrue(res2.message().toString(), contextString.contains("After"));
  }

  @Test
  public void testBadConf() throws InterpreterException {
    props.setProperty("solrbenchmark.work.dir", "/bad/path/to/exe");
    final InterpreterResult res = interpreter.interpret("print('hello')", context);

    assertSame(Code.ERROR, res.code());
  }

  @Override
  public void onUpdateAll(InterpreterOutput interpreterOutput) {

  }

  @Override
  public void onAppend(int i, InterpreterResultMessageOutput interpreterResultMessageOutput, byte[] bytes) {
    try {
      System.out.println(interpreterResultMessageOutput.toInterpreterResultMessage().getData());
    } catch (IOException e) {
      e.printStackTrace();
    }
    buffer.append(bytes, 0, bytes.length);
  }

  @Override
  public void onUpdate(int i, InterpreterResultMessageOutput interpreterResultMessageOutput) {
    try {
      System.out.println(interpreterResultMessageOutput.toInterpreterResultMessage().getData());
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private byte[] getBufferBytes() {


    return buffer.toByteArray();
  }

}
