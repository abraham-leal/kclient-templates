package leal.abraham.streamsExamples;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.log4j.BasicConfigurator;
import org.junit.Test;

import java.util.Arrays;
import java.util.Properties;

import static org.junit.Assert.*;


// testing a KStreams App through Topology Test Driver, this is still ongoing
// TODO Finish this example with more granularity and implement custom processor testing through MockProcessorContext
public class idempotentStreamTest {

    @Test
    public void testMain() {
        BasicConfigurator.configure();

        //Set DSL Builder
        StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> input = builder.stream("input-topic");

        //Build topology
        final KStream<String, String> evaulateRecord = input.flatMapValues(value -> Arrays.asList(value.split("\\s+")));
        evaulateRecord.to("output-topic");
        Topology x = builder.build();

        // Set test props
        Properties testProps = new Properties();
        testProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "testingStreams");
        testProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        testProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        testProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());


        TopologyTestDriver testDriver = new TopologyTestDriver(x,testProps);
        TestInputTopic<String, String> inputTopic = testDriver.createInputTopic("input-topic",Serdes.String().serializer(),Serdes.String().serializer());

        inputTopic.pipeInput("key","Hello my name is Abraham");

        TestOutputTopic<String, String> outputTopic = testDriver.createOutputTopic("output-topic", Serdes.String().deserializer(),Serdes.String().deserializer());

        assertEquals(outputTopic.readKeyValue(), new KeyValue<>("key","Hello"));


    }
}