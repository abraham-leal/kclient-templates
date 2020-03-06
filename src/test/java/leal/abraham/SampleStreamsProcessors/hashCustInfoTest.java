package leal.abraham.SampleStreamsProcessors;

import org.apache.kafka.streams.processor.MockProcessorContext;
import org.apache.kafka.streams.processor.Processor;
import org.junit.Test;

import static org.junit.Assert.*;
// TODO Finish this unit test of a transformer in KStreams
public class hashCustInfoTest {

    @Test
    public void testTransform() {

        final hashCustInfo processorToTest = new hashCustInfo();

        MockProcessorContext testContext = new MockProcessorContext();

        processorToTest.init(testContext);


    }
}