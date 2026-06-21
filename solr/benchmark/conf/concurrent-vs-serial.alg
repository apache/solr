#/**
# * Licensed to the Apache Software Foundation (ASF) under one or more
# * contributor license agreements.  See the NOTICE file distributed with
# * this work for additional information regarding copyright ownership.
# * The ASF licenses this file to You under the Apache License, Version 2.0
# * (the "License"); you may not use this file except in compliance with
# * the License.  You may obtain a copy of the License at
# *
# *     http://www.apache.org/licenses/LICENSE-2.0
# *
# * Unless required by applicable law or agreed to in writing, software
# * distributed under the License is distributed on an "AS IS" BASIS,
# * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# * See the License for the specific language governing permissions and
# * limitations under the License.
# */
# -------------------------------------------------------------------------------------
# multi val params are iterated by NewRound's, added to reports, start with column name.
#


alt.tasks.packages=org.apache.solr.benchmark.byTask.tasks
log.step=5000

docs.file=/data1/enwiki/enwiki-latest-pages-articles.txt
content.source=org.apache.lucene.benchmark.byTask.feeds.LineDocSource


solr.streaming.server.queue.size=10
solr.streaming.server.threadcount=12

solr.internal.server.xmx=1000m

# for remote instance testing
solr.url=http://localhost:8981/solr

#solr.configs.home=conf/solr/configs/filters
#solr.schema=schema:schema_no_filter.xml

solr.field.mappings=doctitle>title_txt_en,docid>id,docname>docname_txt_en,docdate>date_txt_en,docdatenum>datenum_txt_en,doctimesecnum>time_txt_en,body>body_txt_en

query.maker=org.apache.lucene.benchmark.byTask.feeds.EnwikiQueryMaker

# task at this depth or less would print when they start
task.max.depth.log=2

solr.collection=collection1

log.queries=false
# -------------------------------------------------------------------------------------

{ "Bench"

    StopSolrServer
    StartSolrServer(log)
	
	InitSolrBench

  { "Rounds"

    ResetSystemErase
 
    SolrClearIndex
    
    {"AddDocs"
      [ 
          { "MAddDocs" SolrAddDoc > : 15000
 
      ] : 6
      { "BlockTillFinish" BlockSolrServerTillFinish >
    }
    
	SolrCommit
		
    NewRound
  } : 1
  
  StopSolrServer
}


RepSumByPrefRound AddDocs
