package leal.abraham.streamsExamples;

import examples.StockInfo;
import examples.simpleAvroRecord;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import java.util.Map;
import java.util.Properties;

import static org.apache.log4j.Logger.getRootLogger;

public class FilteringKStreamWJoinExample {
    private static String LEFTTOPIC = "someTopic";
    private static String RIGHTTOPIC = "someOtherTopic";
    private static String END = "someEnd";

    public static Properties getConfig (){
        final Properties streamsProps = new Properties();

        //Metadata properties
        streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "leal.abraham.streamsExamples");
        streamsProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsProps.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);

        //Properties to connect to Schema Registry
        streamsProps.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,"sr.superDomain.com");
        streamsProps.put(AbstractKafkaSchemaSerDeConfig.USER_INFO_CONFIG,"superUser:superUser");
        streamsProps.put(AbstractKafkaSchemaSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE,"USER_INFO");

        //Properties to connect to Kafka
        streamsProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka.superDomain.com:9092");
        streamsProps.put(StreamsConfig.SECURITY_PROTOCOL_CONFIG,"SSL");
        streamsProps.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,"/Users/aleal/Documents/Git/operator-1.6/certs/myTruststore.jks");
        streamsProps.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG,"mypass");
        streamsProps.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG,"/Users/aleal/Documents/Git/operator-1.6/certs/testkeystore.p12");
        streamsProps.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "mypass");
        streamsProps.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "PKCS12");


        return streamsProps;
    }

    // This is a value joiner, basically a way to the Kafka Streams what would you like to do in the Join, and
    // what you expect the result to be after the items are joined
    private static ValueJoiner<StockInfo,simpleAvroRecord,String> myJoiner = (lv, rv) -> "This is joined: " + lv.toString() + " " + rv.toString();

    // This is a predicate, basically a boolean condition according to business logic derived from the record
    private static org.apache.kafka.streams.kstream.Predicate<String, StockInfo> onlyAPPL = (key, value) -> value.Symbol == "APPL";

    public static void main(String[] args) {
        BasicConfigurator.configure();
        getRootLogger().setLevel(Level.INFO);

        // We configure serialization/deserialization with Schema Registry, we configure with "false" because the
        // data we are deserializing is not part of a key
        Serde<StockInfo> transientStock = new SpecificAvroSerde<>();
        transientStock.configure((Map)getConfig(), false);
        Serde<simpleAvroRecord> transientAvro = new SpecificAvroSerde<>();
        transientAvro.configure((Map)getConfig(),false);

        // We configure a builder of streaming transformation
        final StreamsBuilder builder = new StreamsBuilder();

        // We start "two consumption streams" of data, defining as well the data we expect from this topic
        // (We expect Strings in the key fields and Avro records in the values)
        final KTable<String, StockInfo> leftinput = builder.table(LEFTTOPIC, Consumed.with(Serdes.String(), transientStock));
        final KTable<String, simpleAvroRecord> rightinput = builder.table(RIGHTTOPIC, Consumed.with(Serdes.String(), transientAvro));


        // This is our "flow"
        KStream<String, String> leftJoinedAndFiltered =
                // We set the stream we want to work off of
                leftinput
                        // We filter so that only records from the stock APPL will go through our flow
                        .filter(onlyAPPL)
                        // We join this record to records from the other stream
                        // This join will be performed according to the contents of the key in the record
                        .leftJoin(rightinput,myJoiner)
                        // As we join, emit the results
                        .toStream();

        // When done, send it to a final topic
        leftJoinedAndFiltered.to(END);

        //Build topology
        final KafkaStreams streams = new KafkaStreams(builder.build(), getConfig());

        try {
            // Start our Streams Application
            streams.start();
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        // Set a hook to close the application gracefully when receiving SIGKILLs
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
