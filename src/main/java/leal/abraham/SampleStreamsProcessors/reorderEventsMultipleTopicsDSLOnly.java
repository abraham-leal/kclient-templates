package leal.abraham.SampleStreamsProcessors;


import examples.reorderThis;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.SerializationException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Merger;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.state.Stores;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded;
import static org.apache.kafka.streams.kstream.Suppressed.untilWindowCloses;

/**
 * This class illustrates how a Kafka Streams app can reorder events coming from multiple topics
 * by a windowed session. This is done by having co-partitioned source topics.
 * The data in the destination will be re-keyed and reorganized from an arbitrary field.
 */
public class reorderEventsMultipleTopicsDSLOnly {

    private static String TOPIC_A = "topicA";
    private static String TOPIC_B = "topicB";
    private static String TOPIC_C = "topicC";
    private static String DESTINATION = "reorderedEvents";
    final static Serde<reorderThis> reorderSerde = new SpecificAvroSerde<>();


    public static Properties getConfig (){
        final Properties streamsProps = new Properties();
        streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "Example");
        streamsProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        streamsProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde.class);
        streamsProps.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);
        streamsProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        streamsProps.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

        return streamsProps;
    }

    public static void main(String[] args) {
        BasicConfigurator.configure();
        Logger.getRootLogger().setLevel(Level.INFO);

        final StreamsBuilder builder = new StreamsBuilder();
        ArrayList<String> topicsToReorder = new ArrayList<String>();

        // Set serde for our value
        Map<String, Object> SRconfig = new HashMap<>();
        SRconfig.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,"http://localhost:8081");
        reorderSerde.configure(SRconfig, false);
        // Set serde for our list
        reorderSerde listSerde = new reorderSerde();
        listSerde.configure(SRconfig,false);

        // Construct topic list to listen on
        topicsToReorder.add(TOPIC_A);
        topicsToReorder.add(TOPIC_B);
        topicsToReorder.add(TOPIC_C);

        // NOTE: If the topics that you are listening on do not share a common value format, you'll have to utilize
        // KGroupedStream#cogroup (group each source topic into a txn key of the same type)
        // Use a simple aggregator to maintain grouped records in a list, then window them.
        // With this implementation, you'll have to implement a SerDe that is able to serialize/deserialize all
        // value types into the underlying state store, and possibly rely on an indicator of which
        // type of record is being flat-mapped in order to access internal fields. (Or possibly, implement
        // a HashMap SerDe that can hold info on the value with keys)


        // Start listening on topics
        builder.stream(topicsToReorder, Consumed.with(Serdes.String(),reorderSerde))
                //make the txnID the key
                .selectKey((k,v) -> v.getTransaction().toString())
                // Group messages by txnID, this will cause messages to be repartitioned, and therefore co-keying is not
                // a requirement.
                .groupByKey(Grouped.with(Serdes.String(),reorderSerde))
                // Set a window of time with an idle period that is acceptable
                // This line can be read as "Create a session window(windowedBy(SessionWindows)) with
                // a period of 60 seconds that a session can be idle before closing (.with(Duration.ofSeconds(60)),
                // after this period, hold a grace period of ZERO for late arriving records (configurable) (.grace(Duration.ZERO))
                .windowedBy(SessionWindows.with(Duration.ofSeconds(60)).grace(Duration.ZERO))
                // Now that we have a session of txnID -> All records within a txnID, we must aggregate them
                // in a convenient way for downstream processing
                // This step can be read as "aggregate the values as a single list, then store them with a String and List serde"
                // The list serde implementation is below this class
                .aggregate(init(),aggregateTransactions(), joinLists(),
                        Materialized
                                .<String,List<reorderThis>>as(Stores.persistentSessionStore("Aggregator", Duration.ofDays(1)))
                        .withKeySerde(Serdes.String())
                        .withValueSerde(listSerde))
                // Now that we have an aggregation of txnID -> List<records>, we must suppress the emision of the records
                // until our window closes to avoid duplicates. By default, KStreams would emit the aggregation every time
                // a new elegible record enters a session
                .suppress(untilWindowCloses(unbounded()))
                // We now convert this to a stream of records
                .toStream()
                // We will now convert the List<records> into individually emitted records
                // Before we do that, we must sort these records the way we want them
                // We store them in a temporary TreeMap for that with order as the key (since we are assured all txns
                // are of the same txnID) After we have the sorted tree, we convert it to a List in order to return the
                // multiple records.
                .flatMap(
                        (k,v) -> {
                            // Sort here
                            ArrayList<KeyValue<String,reorderThis>> toReturn = new ArrayList<>();
                            TreeMap<String,reorderThis> tmpMap = new TreeMap<>();
                            for (reorderThis thisValue : v){
                                tmpMap.put(thisValue.getOrder().toString(),thisValue);
                            }
                            for (Map.Entry<String,reorderThis> entry : tmpMap.entrySet()) {
                                toReturn.add(KeyValue.pair(entry.getValue().getTransaction().toString(), entry.getValue()));
                            }
                            return toReturn;
                        }
                )
                // Finally: We send reordered messages back to a topic
                .to(DESTINATION, Produced.with(Serdes.String(),reorderSerde));

        final KafkaStreams streams = new KafkaStreams(builder.build(), getConfig());

        System.out.println(builder.build().describe());

        try {
            streams.start();
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    // We'll start with an empty list
    static Initializer<List<reorderThis>> init () {
        return ArrayList::new;
    }

    // "For every new value that arrives, just create a list"
    static Aggregator<String, reorderThis, List<reorderThis>> aggregateTransactions() {
        return (key, txn, allTxn) -> {
            allTxn.add(txn);
            return allTxn;
        };
    }

    // "For every two aggregated lists, just combine the two lists"
    static Merger<String, List<reorderThis>> joinLists() {
        return (aggKey, aggOne, aggTwo) -> {
            aggOne.addAll(aggTwo);
            return aggOne;
        };
    }

}

/**
 * This is an implementation of a serde for a list.
 * It is simple to manipulate to depend on any List that has a SpecificAvroSerde
 * as its contents. Just replace "reorderThis" with your avro serde
 */
final class reorderSerde implements Serde<List<reorderThis>> {
    AvroListSerializer mySerializer = new AvroListSerializer();
    AvroListDeserializer myDeserializer = new AvroListDeserializer();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        mySerializer.configure(configs, isKey);
        myDeserializer.configure(configs, isKey);
    }


    @Override
    public Serializer<List<reorderThis>> serializer() {
        return mySerializer;
    }

    @Override
    public Deserializer<List<reorderThis>> deserializer() {
        return myDeserializer;
    }

    final class AvroListSerializer implements Serializer<List<reorderThis>> {

        Serde<reorderThis> reorderSerde = new SpecificAvroSerde<>();

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            reorderSerde.configure(configs, false);
        }

        @Override
        public byte[] serialize(String topic, List<reorderThis> data) {
            if (data == null) return null;

            try(ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos);) {

                final int size = data.size();
                out.writeInt(size);
                for (reorderThis currentTxnOrder : data) {
                    byte [] bytes = reorderSerde.serializer().serialize(topic,currentTxnOrder);
                    out.writeInt(bytes.length);
                    out.write(bytes);
                }
                return baos.toByteArray();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        }
    }

    final class AvroListDeserializer implements Deserializer<List<reorderThis>> {

        Serde<reorderThis> reorderSerde = new SpecificAvroSerde<>();

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            reorderSerde.configure(configs, isKey);
        }

        @Override
        public ArrayList<reorderThis> deserialize(String topic, byte[] data) {
            if (data == null) return new ArrayList<>();
            ArrayList<reorderThis> myList = new ArrayList<>();

            try (final DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data))) {
                final int size = dis.readInt();
                for (int i = 0; i < size; i++) {
                    int entrySize = dis.readInt();
                    byte[] payload = new byte[entrySize];
                    if (dis.read(payload) == -1) {
                        throw new SerializationException("End of the stream was reached prematurely");
                    }
                    myList.add(reorderSerde.deserializer().deserialize(topic, payload));
                }

            } catch (IOException e) {
                e.printStackTrace();
            }

            return myList;
        }
    }
}