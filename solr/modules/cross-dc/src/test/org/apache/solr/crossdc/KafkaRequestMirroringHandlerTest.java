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
package org.apache.solr.crossdc;

import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.crossdc.common.KafkaMirroringSink;
import org.apache.solr.crossdc.common.MirroringException;
import org.apache.solr.crossdc.update.processor.KafkaRequestMirroringHandler;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.apache.solr.SolrTestCaseJ4.assumeWorkingMockito;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class KafkaRequestMirroringHandlerTest {

    @Mock
    private KafkaMirroringSink kafkaMirroringSink;

    @BeforeClass
    public static void ensureWorkingMockito() {
        assumeWorkingMockito();
    }

    @Test
    public void testCheckDeadLetterQueueMessageExecution() throws MirroringException {
        doThrow(MirroringException.class).when(kafkaMirroringSink).submit(any());

        final UpdateRequest updateRequest = new UpdateRequest();
        final KafkaRequestMirroringHandler kafkaRequestMirroringHandler = new KafkaRequestMirroringHandler(kafkaMirroringSink);

        try {
            kafkaRequestMirroringHandler.mirror(updateRequest);
        } catch (MirroringException exception) {
            // do nothing
        }

        verify(kafkaMirroringSink, times(1)).submitToDlq(any());
    }

}
