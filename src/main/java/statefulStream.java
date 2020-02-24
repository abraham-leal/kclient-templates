import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.log4j.BasicConfigurator;

import java.util.Arrays;
import java.util.Properties;

public class statefulStream {

    public static void main(String[] args) {
        BasicConfigurator.configure();

        final String TOPIC = "TestingTopic";
        final String outputTopic = "StreamEnd";

        final Properties streamsProps = new Properties();
        streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "boilerplateStream");
        streamsProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        streamsProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        try {
            final StreamsBuilder builder = new StreamsBuilder();

            final KStream<String, String> messages = builder.stream(TOPIC);

            final KStream<String, String> processing = messages.flatMapValues(value -> Arrays.asList(value.split("\\s+")));

            processing.to(outputTopic);

            final KafkaStreams streams = new KafkaStreams(builder.build(), streamsProps);

            streams.cleanUp();

            streams.start();

            Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

}
