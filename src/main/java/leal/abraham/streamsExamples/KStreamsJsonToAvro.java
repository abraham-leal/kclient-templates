package leal.abraham.streamsExamples;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.log4j.BasicConfigurator;
import examples.customerSubscription;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class KStreamsJsonToAvro {

    private static String sourceTopic = "JsonData";
    private static String destinationTopic = "AvroData";


    public static Properties getConfig (){
        final Properties streamsProps = new Properties();
        streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "leal.abraham.streamsExamples");
        streamsProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "<CCLOUD_DNS>");
        streamsProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsProps.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);
        streamsProps.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,"<CCLOUD_SR_DNS>");
        streamsProps.put(AbstractKafkaAvroSerDeConfig.USER_INFO_CONFIG,"<CCLOUD_SR_KEY>:<CCLOUD_SR_SECRET>");
        streamsProps.put(AbstractKafkaAvroSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE,"USER_INFO");
        streamsProps.put("security.protocol","SASL_SSL");
        streamsProps.put("sasl.mechanism","PLAIN");
        streamsProps.put("ssl.endpoint.identification.algorithm","https");
        streamsProps.put("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule " +
                "required username=\"<API_KEY>\" " +
                "password=\"<API_SECRET>\";");

        return streamsProps;
    }

    public static void main(String[] args) {
        BasicConfigurator.configure();

        final StreamsBuilder builder = new StreamsBuilder();
        JsonParser parser =  new JsonParser();

        // No need to specify Serdes as builder will get the default set in properties
        final KStream<String, String> pipe = builder.stream(sourceTopic);


        // Create custom value mapper that allows to keep a strict one-to-one conversion of records
        // It takes in the JSON String and returns our Avro Object
        KStream<String,customerSubscription> newPipe =  pipe.mapValues((value) -> {
            customerSubscription newRecordValue = new customerSubscription();
            JsonElement jsonElement = parser.parse(value);
            JsonObject jsonObj = jsonElement.getAsJsonObject();

            newRecordValue.setCountry(jsonObj.get("country").getAsString());
            newRecordValue.setCustomer(jsonObj.get("customer").getAsString());
            newRecordValue.setSubscription(jsonObj.get("subscription").getAsInt());

            return newRecordValue;
        });


        // Create SerDe for our Avro Specific Record
        final Map<String, String> serdeConfig = new HashMap<>();
        serdeConfig.put("schema.registry.url","<CCLOUD_SR_DNS>");
        serdeConfig.put("basic.auth.user.info","<CCLOUD_SR_KEY>:<CCLOUD_SR_SECRET>");
        serdeConfig.put("basic.auth.credentials.source","USER_INFO");
        final Serde<customerSubscription> valueSpecificAvroSerde = new SpecificAvroSerde<>();
        valueSpecificAvroSerde.configure(serdeConfig, false);

        // Send the new record back to Kafka
        newPipe.to(destinationTopic, Produced.with(Serdes.String(),valueSpecificAvroSerde));


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
