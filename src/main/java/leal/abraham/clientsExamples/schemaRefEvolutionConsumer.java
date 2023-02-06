package leal.abraham.clientsExamples;

import examples.customerSubscription;
import examples.reorderThis;
import examples.simpleAvroRecord;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.SerializationException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

public class schemaRefEvolutionConsumer {


    private static final String TOPIC = "mainTopic";
    public static  Random getNum = new Random();

    public static Properties getConfig (){
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "pkc-419q3.us-east4.gcp.confluent.cloud:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroDeserializer.class);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "simpleAvroConsumer");
        props.put(ConsumerConfig.GROUP_ID_CONFIG,"Testing12");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,"https://psrc-4r3n1.us-central1.gcp.confluent.cloud");
        props.put(AbstractKafkaAvroSerDeConfig.USER_INFO_CONFIG,"DCKAWZCLFSYJHLG4:k4eCfajP20/W5NvYoovI4YbXytAX7DDYLgOSS6jqDeYe17JstAUqczox4PHiqEKu");
        props.put(AbstractKafkaAvroSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE,"USER_INFO");
        props.put("auto.register.schemas","false");
        props.put("use.latest.version","true");
        props.put("normalize.schemas", "false");

        props.put("security.protocol","SASL_SSL");
        props.put("sasl.mechanism","PLAIN");
        props.put("ssl.endpoint.identification.algorithm","https");
        props.put("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule " +
                "required username=\"LPUQ7L4OASVIHEFO\" " +
                "password=\"7dinYNPVL5sJu8P4Jr0KvbGUCohDrsVKDR9hL+kP7LSe3ofvXWbNuK8nl508Fl5p\";");

        return props;
    }

    public static void main(final String[] args) throws ExecutionException {
        BasicConfigurator.configure();
        Logger.getRootLogger().setLevel(Level.INFO);

        KafkaConsumer<String,GenericRecord> consumerGeneric = new KafkaConsumer<>(getConfig());
        consumerGeneric.subscribe(Collections.singleton("mainTopic"));


        try {
            while(true){
                ConsumerRecords<String,GenericRecord> records = consumerGeneric.poll(Duration.ofMillis(10000));
                for (ConsumerRecord<String,GenericRecord> record : records){
                    System.out.println("Generic record received: " + record.value().toString());
                }
            }
        } catch (SerializationException a){
            a.printStackTrace();
        }

        //Shutdown hook to assure producer close
        Runtime.getRuntime().addShutdownHook(new Thread(consumerGeneric::close));
    }

    public static Integer getID (){
        return getNum.nextInt(10000 - 1000) + 1000;
    }

    public static String getName(){
        String [] x = {"Abraham", "Russell", "Bob", "Mark","Jay","Valeria", "Miguel", "Jaime"};
        return x[getNum.nextInt(x.length)];
    }

    public static String getCountry(){
        String [] x = {"USA", "Mexico", "Canada", "Norway","Argentina","England"};
        return x[getNum.nextInt(x.length)];
    }

    public static Integer getSub (){
        return getNum.nextInt(79999 - 7000) + 70000;
    }


}
