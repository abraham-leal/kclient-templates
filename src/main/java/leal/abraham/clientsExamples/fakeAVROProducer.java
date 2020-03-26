package leal.abraham.clientsExamples;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import examples.ExtraInfo;
import org.apache.commons.lang3.SerializationException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.BasicConfigurator;

import java.util.*;

public class fakeAVROProducer {


    private static final String TOPIC = "fake";
    private static final int recordsToGenerate = 10000000;
    public static  Random getNum = new Random();

    public static Properties getConfig (){
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 5);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "AvroProducer");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 5);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5000);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        props.put(ProducerConfig.SECURITY_PROVIDERS_CONFIG,"");
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,"");
        props.put(AbstractKafkaAvroSerDeConfig.USER_INFO_CONFIG,"");
        props.put(AbstractKafkaAvroSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE,"USER_INFO");
        props.put("security.protocol","SASL_SSL");
        props.put("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required username=\"\" password=\"\";");
        props.put("ssl.endpoint.identification.algorithm","https");
        props.put("sasl.mechanism","PLAIN");

        return props;
    }

    public static void main(final String[] args) {
        BasicConfigurator.configure();

        KafkaProducer<String, ExtraInfo> producer = new KafkaProducer<String, ExtraInfo>(getConfig());

        try {

            for (int count = 0; count < recordsToGenerate; count++) {
                final ExtraInfo recordValue = new ExtraInfo(getID(), getName(), getFood(), getPC());
                final ProducerRecord<String, ExtraInfo> record = new ProducerRecord<String, ExtraInfo>(TOPIC, null, recordValue);
                producer.send(record);
                Thread.sleep(1000L);
            }

            //Tell producer to flush before exiting
            producer.flush();
            System.out.printf("Successfully produced %s%n messages to a topic called %s%n", recordsToGenerate, TOPIC);


        } catch (SerializationException | InterruptedException a){
            a.printStackTrace();
        }

        //Shutdown hook to assure producer close
        Runtime.getRuntime().addShutdownHook(new Thread(producer::close));

    }

    public static Integer getID (){
        return getNum.nextInt(10000 - 1000) + 1000;
    }

    public static String getName(){
        String [] x = {"Abraham", "Russell", "Bob", "Mark","Jay","Valeria", "Miguel", "Jaime"};
        return x[getNum.nextInt(x.length)];
    }

    public static String getFood(){
        String [] x = {"Hot Dog", "Pizza", "Fries", "Burger","Chicken Strips","Salad"};
        return x[getNum.nextInt(x.length)];
    }

    public static Integer getPC (){
        return getNum.nextInt(79999 - 7000) + 70000;
    }


}
