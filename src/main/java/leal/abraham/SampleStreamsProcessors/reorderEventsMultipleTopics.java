package leal.abraham.SampleStreamsProcessors;


import examples.reorderThis;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.confluent.ps.ServingRecord;
import io.confluent.ps.Tbf0Prescriber;
import io.confluent.ps.Tbf0Rx;
import io.confluent.ps.Tbf0RxTransaction;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.internals.KeyValueStoreBuilder;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * This class illustrates how a Kafka Streams app can reorder events coming from multiple topics
 * by a windowed join. This is done by having co-partitioned, co-keyed source topics.
 * The data in the destination will be re-keyed and reorganized from an arbitrary field.
 */
public class reorderEventsMultipleTopics {

    private static String TOPIC_A = "topicA";
    private static String TOPIC_B = "topicB";
    private static String TOPIC_C = "topicC";
    private static String DESTINATION = "reorderedEvents";


    public static Properties getConfig (){
        final Properties streamsProps = new Properties();
        streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "leal.abraham.examples.reorderingEvents");
        streamsProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        streamsProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsProps.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);

        return streamsProps;
    }

    public static void main(String[] args) {
        BasicConfigurator.configure();
        Logger.getRootLogger().setLevel(Level.INFO);

        final StreamsBuilder builder = new StreamsBuilder();
        ArrayList<String> topicsToReorder = new ArrayList<String>();

        // Set serde for our value
        final Serde<reorderThis> reorderSerde = new SpecificAvroSerde<>();
        Map<String, Object> SRconfig = new HashMap<>();
        SRconfig.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,"http://localhost:8081");
        reorderSerde.configure(
                SRconfig,
                false);

        // construct in-memory store for persistence of state
        StoreBuilder<KeyValueStore<String,reorderThis>> myStore = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("reorder"),
                Serdes.String(),
                reorderSerde);

        // Construct topic list to listen on
        topicsToReorder.add(TOPIC_A);
        topicsToReorder.add(TOPIC_B);
        topicsToReorder.add(TOPIC_C);

        // Register the previously build state store with our topology
        // Set builder with right serdes
        // Send the record through our transform code
        // Send to topic
        builder.addStateStore(myStore).stream(topicsToReorder, Consumed.with(Serdes.String(),reorderSerde))
                //reorder messages through processor API
                .transform(new reorderOnCustomField(), "reorder")
                // Send reordered messages back to a topic
                .to(DESTINATION, Produced.with(Serdes.String(),reorderSerde));

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
