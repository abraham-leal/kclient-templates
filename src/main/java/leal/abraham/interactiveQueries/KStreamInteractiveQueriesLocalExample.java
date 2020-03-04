package leal.abraham.interactiveQueries;
// In this example, there will be a KTable containing reference data from a Kafka Topic (Key, UserID)
// and a KStream containing more information (Key, name, country, value)
// We will be joining by key to get both information together

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.log4j.BasicConfigurator;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.util.Properties;

public class KStreamInteractiveQueriesLocalExample {

    private static String LEFTTOPIC = "fakeJSONdata";
    private static String RIGHTTOPIC = "fakeJSONref";


    public static Properties getConfig (){
        final Properties streamsProps = new Properties();
        streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "leal.abraham.examples.QueryStore2");
        streamsProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        streamsProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsProps.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);
        streamsProps.put("auto.offset.reset","earliest");

        return streamsProps;
    }

    public static void main(String[] args){
        BasicConfigurator.configure();

        //Custom Value Joined to achieve our goal while keeping the JSON format in the message
        ValueJoiner<String,String,String> joinMessage = new ValueJoiner<String, String, String>() {
            @Override
            public String apply(String leftValue, String rightValue) {
                try {
                    Object userDetailsObj = new JSONParser().parse(leftValue);
                    JSONObject userDetails = (JSONObject) userDetailsObj;
                    userDetails.put("userid",rightValue.toString());
                    return userDetails.toJSONString();
                }
                catch (Exception e){
                    System.out.println("Could Not Parse and add to JSON from left stream");
                    e.printStackTrace();
                    System.exit(1);
                }

                return "Invalid";
            }
        };


        final StreamsBuilder builder = new StreamsBuilder();

        // No need to specify Serdes as builder will get the default set in properties
        // Have to use GlobalKTable in order to bootstrap the values from the reference topic
        final GlobalKTable<String, String> rightinput = builder.globalTable(RIGHTTOPIC, Materialized.as("sampleStore"));
        final KStream<String, String> leftinput = builder.stream(LEFTTOPIC);

        // Joining on Primary key, inner join KStream-KTable will only keep exactly matched records, with a custom join condition, no null keys
        KStream<String, String> innerJoined = leftinput.join(rightinput,(lk,lv) -> lk, joinMessage);
        //Build topology
        final KafkaStreams streams = new KafkaStreams(builder.build(), getConfig());

        try {
            streams.start();
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        try{
            queryState(streams);
        }catch(Exception f){
            f.printStackTrace();
        }

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    public static void queryState(KafkaStreams streams) throws InterruptedException {

        if (streams.state().isRunning()){
            ReadOnlyKeyValueStore<String,Long> kvstore = streams.store("sampleStore",QueryableStoreTypes.keyValueStore());
            System.out.println();
            System.out.println();
            //Querying a key that was in the topic
            System.out.println("Current value of key in store:" + kvstore.get("69e66189-77a0-4818-aeb0-685237aba77d"));
            System.out.println();
            System.out.println();
        }

    }

}
