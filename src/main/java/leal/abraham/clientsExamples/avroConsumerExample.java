package leal.abraham.clientsExamples;

import examples.simpleAvroRecord;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.commons.lang3.SerializationException;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class avroConsumerExample {

    public static Properties getConfig (){
        final Properties props = new Properties();
        // Consumption properties for connection
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "XXXXXX");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "simpleAvroConsumer");
        props.put(ConsumerConfig.GROUP_ID_CONFIG,"Testing");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,"SASL_SSL");
        props.put(SaslConfigs.SASL_MECHANISM,"PLAIN");
        props.put(SaslConfigs.SASL_JAAS_CONFIG,"org.apache.kafka.common.security.plain.PlainLoginModule " +
                "required username=\"XXXX\" " +
                "password=\"XXX\";");

        // Consumption properties for schema and deserialization
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,"XXX");
        props.put(AbstractKafkaAvroSerDeConfig.USER_INFO_CONFIG,"XXX:XXX");
        props.put(AbstractKafkaAvroSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE,"USER_INFO");

        return props;
    }

    public static void main(final String[] args) {
        BasicConfigurator.configure();
        Logger.getRootLogger().setLevel(Level.INFO);

        Properties consumer2Properties = getConfig();
        consumer2Properties.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG,"true");

        KafkaConsumer<String, simpleAvroRecord> consumerSpecific = new KafkaConsumer<>(consumer2Properties);
        consumerSpecific.subscribe(Collections.singleton("avroEvolve"));
        AvroSpecificConsumption(consumerSpecific);

        System.out.println("Successfully consumed three records.");

        //Shutdown hook to assure producer close
        Runtime.getRuntime().addShutdownHook(new Thread(consumerSpecific::close));

    }
    public static void AvroSpecificConsumption(KafkaConsumer<String,simpleAvroRecord> reusableConsumer){

        try {
            while(true){
                ConsumerRecords<String, simpleAvroRecord> records = reusableConsumer.poll(Duration.ofMillis(50));
                for (ConsumerRecord<String, simpleAvroRecord> record : records) {
                    System.out.println("Specific record received: " + record.value().toString());
                }
            }
        } catch (SerializationException a){
            a.printStackTrace();
        }
    }

}

