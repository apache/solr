package org.apache.solr.servlet;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

import static org.apache.solr.servlet.ServletUtils.closeShield;

public class AdminServlet extends HttpServlet {
  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    req = closeShield(req, false);
    resp = closeShield(resp, false);
    super.doGet(req, resp);
  }

  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    req = closeShield(req, false);
    resp = closeShield(resp, false);
    super.doPost(req, resp);
  }
}
