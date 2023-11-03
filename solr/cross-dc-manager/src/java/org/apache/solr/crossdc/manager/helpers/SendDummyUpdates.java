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
package org.apache.solr.crossdc.manager.helpers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.crossdc.common.MirroredSolrRequest;
import org.apache.solr.crossdc.common.MirroredSolrRequestSerializer;

import java.util.Properties;

public class SendDummyUpdates {
    public static void main(String[] args) {
        String TOPIC = "Trial";
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("acks", "all");
        properties.put("retries", 3);
        properties.put("batch.size", 16384);
        properties.put("buffer.memory", 33554432);
        properties.put("linger.ms", 1);
        properties.put("key.serializer", StringSerializer.class.getName());
        properties.put("value.serializer", MirroredSolrRequestSerializer.class.getName());
        Producer<String, MirroredSolrRequest> producer = new KafkaProducer(properties);
        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.add("id", String.valueOf(System.currentTimeMillis()));
        MirroredSolrRequest mirroredSolrRequest = new MirroredSolrRequest(updateRequest);
        System.out.println("About to send producer record");
        producer.send(new ProducerRecord(TOPIC, mirroredSolrRequest));
        System.out.println("Sent producer record");
        producer.close();
        System.out.println("Closed producer");
    }
}
