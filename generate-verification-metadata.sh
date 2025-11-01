#!/bin/bash

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
