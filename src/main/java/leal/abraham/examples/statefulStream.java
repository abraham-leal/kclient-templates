package leal.abraham.examples;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.log4j.BasicConfigurator;

import java.util.Arrays;
import java.util.Properties;

public class statefulStream {

    private static String TOPIC = "statefulTopic";
    private static String OUTPUT_TOPIC = "StreamEnd";
    private static String STORE = "InternalStore";


    public static Properties getConfig (){
        final Properties streamsProps = new Properties();
        streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "boilerplateStream");
        streamsProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        streamsProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsProps.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/state-store");

        return streamsProps;
    }

    public static void main(String[] args) {
        BasicConfigurator.configure();

        final StreamsBuilder builder = new StreamsBuilder();

        //Build Internal, persistent store, with String Serdes for both key and value, if wanting to use only an in-memory store
        // look into inMemoryKeyValueStore
        StoreBuilder<KeyValueStore<String,String>> storedState =
                Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(STORE),Serdes.String(),Serdes.String());

        //Attach topology to state store
        builder.addStateStore(storedState);

        // No need to specify Serdes as builder will get the default set in properties
        final KStream<String, String> input = builder.stream(TOPIC);

        //Build topology
        final KStream<String, String> [] evaulateRecord = input.branch(
                (key, value) -> key.startsWith("1"),
                (key, value) -> key.startsWith("2")
        );

        //The count aggregation will utilized the specified store, rather than abstracting one
        //Otherwise, it would abstract away the store and create two stores
        final KTable<String,Long> OneKeys = evaulateRecord[0].groupByKey().count();
        final KTable<String,Long> TwoKeys = evaulateRecord[1].groupByKey().count();

        OneKeys.toStream().to(OUTPUT_TOPIC);
        TwoKeys.toStream().to(OUTPUT_TOPIC);

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
