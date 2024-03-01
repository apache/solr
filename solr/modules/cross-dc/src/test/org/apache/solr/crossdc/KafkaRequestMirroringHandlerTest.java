package org.apache.solr.crossdc;

import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.crossdc.common.KafkaMirroringSink;
import org.apache.solr.crossdc.common.MirroringException;
import org.apache.solr.crossdc.update.processor.KafkaRequestMirroringHandler;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class KafkaRequestMirroringHandlerTest {

    @Mock
    private KafkaMirroringSink kafkaMirroringSink;

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
