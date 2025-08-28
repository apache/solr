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
package org.apache.jmh;

import jakarta.servlet.ServletConfig;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.apache.zeppelin.interpreter.InterpreterContext;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ConcurrentHashMap;
import java.util.jar.Manifest;

public class StatusServlet extends HttpServlet {

  public StatusServlet() {
    super();
  }

  @Override public void init(ServletConfig config) throws ServletException {
    super.init(config);
  }

  @Override
  public void init() throws ServletException {
    /* Nothing to do */
  }

  @Override
  public void destroy() {
    /* Nothing to do */
  }

  protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
    response.setContentType("text/html; charset=UTF-8");
    response.setStatus(HttpServletResponse.SC_OK);
    ConcurrentHashMap<String,InterpreterContext> contextMap = (ConcurrentHashMap<String,InterpreterContext>) request.getServletContext()
        .getAttribute("contextMap");
    response.getWriter().println("# in contextMap=" + contextMap.size());

    contextMap.forEach((s, interpreterContext) -> {
      try {
        response.getWriter().println("LocalProperties: " + s + " ->" + interpreterContext.getLocalProperties());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });

    response.getWriter().println("MapStore: " + request.getServletContext().getAttribute("artifactChaff"));


    try {
    //  ServletContext context = request.getServletContext();
      InputStream inputStream = JMHInterpreter.class.getResourceAsStream("META-INF/MANIFEST.MF");
      Manifest manifest = new Manifest(inputStream);
      // do stuff with it
      // Attributes attr = manifest.getMainAttributes();
      // String value = attr.getValue("Manifest-Version");
      response.getWriter().println("Manifest:\n" + manifest.getEntries());


    } catch (IOException E) {
      // handle
    }

  }
}
