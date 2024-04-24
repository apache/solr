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

# ./gradlew dev

echo solr/packaging/build/dev/bin/solr start -cloud -noprompt

#
# conventional search: dinner collection with dishes ...
#

solr/packaging/build/dev/bin/solr create -c dinner --shards 1 --replicationFactor 1

sleep 3

curl -X POST 'http://localhost:8983/solr/dinner/update?commitWithin=1234' -H 'Content-Type: application/json' --data-binary '[{"id":"bp", "text_ws":"banoffee pie"}]'

curl -X POST 'http://localhost:8983/solr/dinner/update?commitWithin=1234' -H 'Content-Type: application/json' --data-binary '[{"id":"cp", "text_ws":"chocolate pudding"}]'

curl -X POST 'http://localhost:8983/solr/dinner/update?commitWithin=1234' -H 'Content-Type: application/json' --data-binary '[{"id":"fp", "text_ws":"fish pie"}]'

sleep 3

#
# conventional search: ... people search for dishes
#

curl --silent "http://localhost:8983/solr/dinner/select?fl=id,text_ws&q=*:*"

curl --silent "http://localhost:8983/solr/dinner/select?fl=id,text_ws&q=text_ws:fish"

curl --silent "http://localhost:8983/solr/dinner/select?fl=id,text_ws&q=text_ws:pie"

#
# reverse search: collection with people's saved searches for dishes ...
#

solr/packaging/build/dev/bin/solr create -c saved_dinner_searches --shards 1 --replicationFactor 1

sleep 3

curl -X POST 'http://localhost:8983/solr/saved_dinner_searches/update?commitWithin=1234' -H 'Content-Type: application/json' --data-binary '[{"id":"Alex", "q_s":"*:*" }]'

curl -X POST 'http://localhost:8983/solr/saved_dinner_searches/update?commitWithin=1234' -H 'Content-Type: application/json' --data-binary '[{"id":"Fran", "q_s":"text_ws:fish" }]'

curl -X POST 'http://localhost:8983/solr/saved_dinner_searches/update?commitWithin=1234' -H 'Content-Type: application/json' --data-binary '[{"id":"Pat", "q_s":"text_ws:pie" }]'

sleep 3

#
# reverse search: ... given a document about a dish, whose saved search does it match?
#

curl -X POST 'http://localhost:8983/solr/saved_dinner_searches/reverse_select' --data 'doc={"id":"cp", "text_ws":"chocolate pudding"}'
echo "should match Alex only"

curl -X POST 'http://localhost:8983/solr/saved_dinner_searches/reverse_select' --data 'doc={"id":"fp", "text_ws":"fish pie"}'
echo "should match Fran and Pat and Alex"

curl -X POST 'http://localhost:8983/solr/saved_dinner_searches/reverse_select' --data 'doc={"id":"bp", "text_ws":"banoffee pie"}'
echo "should match Pat and Alex"

#
# reverse search: remove Alex's saved search ...
#

curl -X POST 'http://localhost:8983/solr/saved_dinner_searches/update?commitWithin=1234' -H 'Content-Type: application/json' --data-binary '{ "delete" : { "id" : "Alex" }}'

sleep 3

#
# reverse search: ... and now who wants chocolate pudding?
#

curl -X POST 'http://localhost:8983/solr/saved_dinner_searches/reverse_select' --data 'doc={"id":"cp", "text_ws":"chocolate pudding"}'
echo "should match no one"

echo solr/packaging/build/dev/bin/solr stop -all

