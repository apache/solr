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
package org.apache.solr.handler;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.junit.Assert;

public class TestRestoreCoreUtil {
  public static boolean fetchRestoreStatus (String baseUrl, String coreName) throws IOException {
    String leaderUrl = baseUrl + "/" + coreName +
        ReplicationHandler.PATH + "?wt=xml&command=" + ReplicationHandler.CMD_RESTORE_STATUS;
    final Pattern pException = Pattern.compile("<str name=\"exception\">(.*?)</str>");

    InputStream stream = null;
    try {
      URL url = new URL(leaderUrl);
      stream = url.openStream();
      String response = IOUtils.toString(stream, "UTF-8");
      Matcher matcher = pException.matcher(response);
      if(matcher.find()) {
        Assert.fail("Failed to complete restore action with exception " + matcher.group(1));
      }
      if(response.contains("<str name=\"status\">success</str>")) {
        return true;
      } else if (response.contains("<str name=\"status\">failed</str>")){
        Assert.fail("Restore Failed");
      }
      stream.close();
    } finally {
      IOUtils.closeQuietly(stream);
    }
    return false;
  }
}
