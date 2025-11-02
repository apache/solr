#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Open the output file as file descriptor 3
exec 3> gradle/verification-metadata.xml

# Write the XML header
cat >&3 <<EOL
<?xml version="1.0" encoding="UTF-8"?>
<verification-metadata xmlns="https://schema.gradle.org/dependency-verification"
  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  xsi:schemaLocation="https://schema.gradle.org/dependency-verification https://schema.gradle.org/dependency-verification/dependency-verification-1.3.xsd">
  <configuration>
    <verify-metadata>true</verify-metadata>
    <verify-signatures>false</verify-signatures>
  </configuration>
  <components>
EOL

# Loop through the GAVF entries and write each component
old_gav=""
while IFS=: read -r g a v f || [[ -n $g ]]; do
  sha1=$(< "solr/licenses/$f.sha1")
  rm -f "solr/licenses/$f.sha1"

  # New component
  if [ "$g:$a:$v" != "$old_gav" ]; then
    # Close the previous component if it exists
    if [ -n "$old_gav" ]; then
      echo "    </component>"                                           >&3
    fi
    echo "    <component group=\"$g\" name=\"$a\" version=\"$v\">"      >&3
  fi
  echo "      <artifact name=\"$f\">"                                   >&3
  echo "        <sha1 value=\"$sha1\" origin=\"solr/license folder\"/>" >&3
  echo "      </artifact>"                                              >&3
  old_gav="$g:$a:$v"
done < checksums.txt

# Write the XML footer
cat >&3 <<EOL
    </component>
  </components>
</verification-metadata>
EOL
