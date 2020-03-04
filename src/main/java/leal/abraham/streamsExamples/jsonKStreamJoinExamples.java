package leal.abraham.streamsExamples;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.log4j.BasicConfigurator;

import java.time.Duration;
import java.util.Properties;

public class jsonKStreamJoinExamples {

    private static String LEFTTOPIC = "fakeJSON1";
    private static String RIGHTTOPIC = "fakeJSON2";
    private static String RIGHTTOPIC2 = "fakeJSON3";
    private static String OUTPUT_TOPIC = "JoinedStreams";


    public static Properties getConfig (){
        final Properties streamsProps = new Properties();
        streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "leal.abraham.examples.jsonStreamProcessor3");
        streamsProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        streamsProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsProps.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);

        return streamsProps;
    }

    public static void main(String[] args) {
        BasicConfigurator.configure();

        final StreamsBuilder builder = new StreamsBuilder();

        // No need to specify Serdes as builder will get the default set in properties
        final KStream<String, String> leftinput = builder.stream(LEFTTOPIC);
        final KStream<String, String> rightinput = builder.stream(RIGHTTOPIC);
        final KStream<String, String> rightinput2 = builder.stream(RIGHTTOPIC2);

        // Joining on Primary key, inner join KStream-KStream will only keep exactly matched records, with a lambda expression as a join condition, no null keys
        KStream<String, String> innerJoined =
                leftinput.join(rightinput, (lv, rv) -> "[" + lv + "," + rv + "]",
                JoinWindows.of(Duration.ofMillis(100)));

        // Joining on Primary key, left join join KStream-KStream keeps records from left that don't match right, with a lambda expression as a join condition, no null keys
        KStream<String, String> leftJoined =
                leftinput.leftJoin(rightinput2, (lv, rv) -> "[" + lv + "," + rv + "]",
                        JoinWindows.of(Duration.ofMillis(100)));

        // Joining on Primary key, outer join KStream-KStream will keep all records that match in either stream, with a lambda expression as a join condition, no null keys
        KStream<String, String> outerJoined =
                leftinput.outerJoin(rightinput2, (lv, rv) -> "[" + lv + "," + rv + "]",
                        JoinWindows.of(Duration.ofMillis(100)));

        // Joining on Foreign Key, causing repartition


        // Print the joins
        //innerJoined.print(Printed.toSysOut());
        //leftJoined.print(Printed.toSysOut());
        //outerJoined.print(Printed.toSysOut());


        //Build topology

        final KafkaStreams streams = new KafkaStreams(builder.build(), getConfig());

        try {
            streams.start();
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

}
