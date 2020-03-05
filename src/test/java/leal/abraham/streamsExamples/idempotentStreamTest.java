package leal.abraham.streamsExamples;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.log4j.BasicConfigurator;
import org.junit.Test;

import java.util.Arrays;
import java.util.Properties;

import static org.junit.Assert.*;


// testing a KStreams Topology through Topology Test Driver
public class idempotentStreamTest {

    @Test
    public void testSplitValueBySpace() {
        BasicConfigurator.configure();

        //Build topology, pass the stream to the topology in code
        StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> input = builder.stream("input-topic");
        final KStream<String, String> evaulateRecord = idempotentStream.splitValueBySpace(input);
        evaulateRecord.to("output-topic");
        Topology localTopology = builder.build();

        //Gets properties from local class
        TopologyTestDriver testDriver = new TopologyTestDriver(localTopology,generateLocalProperties.commonPropsString());

        //Test Driver generates input topic
        TestInputTopic<String, String> inputTopic = testDriver.createInputTopic("input-topic",Serdes.String().serializer(),Serdes.String().serializer());

        //Fake record input
        inputTopic.pipeInput("key","Hello my name is Abraham");

        //Test Driver generates output topic
        TestOutputTopic<String, String> outputTopic = testDriver.createOutputTopic("output-topic", Serdes.String().deserializer(),Serdes.String().deserializer());


        //Asserts for test
        assertEquals(outputTopic.readKeyValue(), new KeyValue<>("key","Hello"));
        assertEquals(outputTopic.readKeyValue(), new KeyValue<>("key","my"));
        assertEquals(outputTopic.readKeyValue(), new KeyValue<>("key","name"));
        assertEquals(outputTopic.readKeyValue(), new KeyValue<>("key","is"));
        assertEquals(outputTopic.readKeyValue(), new KeyValue<>("key","Abraham"));


    }
}