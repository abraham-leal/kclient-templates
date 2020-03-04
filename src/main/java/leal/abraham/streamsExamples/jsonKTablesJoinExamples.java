package leal.abraham.streamsExamples;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.log4j.BasicConfigurator;

import java.util.Properties;

public class jsonKTablesJoinExamples {

    private static String LEFTTOPIC = "fakeJSON1";
    private static String RIGHTTOPIC = "fakeJSON2";


    public static Properties getConfig (){
        final Properties streamsProps = new Properties();
        streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "leal.abraham.examples.jsonTableProcessor");
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
        final KTable<String, String> leftinput = builder.table(LEFTTOPIC);
        final KTable<String, String> rightinput = builder.table(RIGHTTOPIC);

        // Joining on Primary key, inner join KStream-KStream will only keep exactly matched records, with a lambda expression as a join condition, no null keys
        KTable<String, String> innerJoined =
                leftinput.join(rightinput, (lv, rv) -> "[" + lv + "," + rv + "]");

        // Joining on Primary key, left join join KStream-KStream keeps records from left that don't match right, with a lambda expression as a join condition, no null keys
        KTable<String, String> leftJoined =
                leftinput.leftJoin(rightinput, (lv, rv) -> "[" + lv + "," + rv + "]");

        // Joining on Primary key, outer join KStream-KStream will keep all records that match in either stream, with a lambda expression as a join condition, no null keys
        KTable<String, String> outerJoined =
                leftinput.outerJoin(rightinput, (lv, rv) -> "[" + lv + "," + rv + "]");

        // Joining on Foreign Key, causing repartition


        // Print the joins
        //innerJoined.toStream().print(Printed.toSysOut());
        //leftJoined.toStream().print(Printed.toSysOut());
        //outerJoined.toStream().print(Printed.toSysOut());


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
