package org.apache.solr.servlet;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.regex.Pattern;

import static org.apache.solr.servlet.ServletUtils.closeShield;
import static org.apache.solr.servlet.ServletUtils.configExcludes;
import static org.apache.solr.servlet.ServletUtils.excludedPath;

@WebServlet
public class AdminServlet extends HttpServlet implements PathExcluder{
  private ArrayList<Pattern> excludes;

  @Override
  public void init() throws ServletException {
    configExcludes(this, getInitParameter("excludePatterns"));
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    req = closeShield(req, false);
    resp = closeShield(resp, false);
    if (excludedPath(excludes,req,resp,null)) {
      return;
    }
    super.doGet(req, resp);
  }

  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    req = closeShield(req, false);
    resp = closeShield(resp, false);
    if (excludedPath(excludes,req,resp,null)) {
      return;
    }
    super.doPost(req, resp);
  }

  @Override
  public void setExcludePatterns(ArrayList<Pattern> excludePatterns) {
    this.excludes = excludePatterns;
  }
}
