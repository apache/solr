package org.apache.solr.client.solrj.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.solr.common.util.SuppressForbidden;

public class DebugServlet extends HttpServlet {
  public static void clear() {
    lastMethod = null;
    headers = null;
    parameters = null;
    errorCode = null;
    queryString = null;
    cookies = null;
    responseHeaders = null;
    responseBody = null;
  }

  public static Integer errorCode = null;
  public static String lastMethod = null;
  public static HashMap<String, String> headers = null;
  public static Map<String, String[]> parameters = null;
  public static String queryString = null;
  public static javax.servlet.http.Cookie[] cookies = null;
  public static List<String[]> responseHeaders = null;
  public static Object responseBody = null;

  public static void setErrorCode(Integer code) {
    errorCode = code;
  }

  public static void addResponseHeader(String headerName, String headerValue) {
    if (responseHeaders == null) {
      responseHeaders = new ArrayList<>();
    }
    responseHeaders.add(new String[] {headerName, headerValue});
  }

  @Override
  protected void doDelete(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    lastMethod = "delete";
    recordRequest(req, resp);
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    lastMethod = "get";
    recordRequest(req, resp);
  }

  @Override
  protected void doHead(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    lastMethod = "head";
    recordRequest(req, resp);
  }

  private void setHeaders(HttpServletRequest req) {
    Enumeration<String> headerNames = req.getHeaderNames();
    headers = new HashMap<>();
    while (headerNames.hasMoreElements()) {
      final String name = headerNames.nextElement();
      headers.put(name.toLowerCase(Locale.getDefault()), req.getHeader(name));
    }
  }

  @SuppressForbidden(reason = "fake servlet only")
  private void setParameters(HttpServletRequest req) {
    parameters = req.getParameterMap();
  }

  private void setQueryString(HttpServletRequest req) {
    queryString = req.getQueryString();
  }

  private void setCookies(HttpServletRequest req) {
    javax.servlet.http.Cookie[] ck = req.getCookies();
    cookies = req.getCookies();
  }

  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    lastMethod = "post";
    recordRequest(req, resp);
  }

  @Override
  protected void doPut(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    lastMethod = "put";
    recordRequest(req, resp);
  }

  private void recordRequest(HttpServletRequest req, HttpServletResponse resp) {
    setHeaders(req);
    setParameters(req);
    setQueryString(req);
    setCookies(req);
    if (responseHeaders != null) {
      for (String[] h : responseHeaders) {
        resp.addHeader(h[0], h[1]);
      }
    }
    if (responseBody != null) {
      try {
        if (responseBody instanceof String) {
          resp.getWriter().print((String) responseBody);
        } else if (responseBody instanceof byte[]) {
          resp.getOutputStream().write((byte[]) responseBody);
        } else {
          throw new IllegalArgumentException(
              "Only String and byte[] are supported for responseBody.");
        }
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }
    if (null != errorCode) {
      try {
        resp.sendError(errorCode);
      } catch (IOException e) {
        throw new RuntimeException("sendError IO fail in DebugServlet", e);
      }
    }
  }
}
