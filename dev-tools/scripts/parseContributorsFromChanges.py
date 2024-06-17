#  Licensed to the Apache Software Foundation (ASF) under one or more
#  contributor license agreements.  See the NOTICE file distributed with
#  this work for additional information regarding copyright ownership.
#  The ASF licenses this file to You under the Apache License, Version 2.0
#  (the "License"); you may not use this file except in compliance with
#  the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import sys
import re
from collections import defaultdict

# Read data from standard input
data = sys.stdin.read()

# Replace all carriage return line feed (Windows) with line feed
data = data.replace('\r\n', '\n')

# Replace all carriage return (Mac OS before X) with line feed
data = data.replace('\r', '\n')

# Split data at blank lines
paras = data.split('\n\n')

# Initialize a default dictionary to store contributors and their counts
contributors = defaultdict(int)

# Regular expression to find the attribution in parentheses at the end of a line
pattern = re.compile(r"\(([^()]*)\)$")

for para in paras:
  # Normalize whitespace (replace all whitespace with a single space)
  para = re.sub('\s+', ' ', para).strip()
  #print(f'> {para}')

  # Find all contributors in the line
  match = pattern.search(para.strip())
  if match:
    attribution = match.group(1)
    # might have a "via" committer; we only want the author here
    attribution = attribution.split(" via ")[0] # keep left side
    # Split the contributors by comma and strip whitespace
    for contributor in attribution.split(','):
      contributor = contributor.strip()
      contributors[contributor] += 1

del contributors['solrbot']

sorted_contributors = sorted(contributors.items(), key=lambda item: item[1], reverse=True)

# Print the contributors and their counts
for contributor, count in sorted_contributors:
  print(f'{contributor}: {count}')

print('\n\nThanks to all contributors!: ')
print(', '.join([contributor for contributor, count in sorted_contributors]))