package leal.abraham.clientsExamples;

import examples.ExtraInfo;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import org.apache.commons.lang3.SerializationException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.util.Properties;
import java.util.Random;

public class fakeAVROProducerCCloud {


    private static final String TOPIC = "myinternaltopic";
    private static final int recordsToGenerate = 1000000000;
    public static  Random getNum = new Random();

    public static Properties getConfig (){
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "pkc-lgk0v.us-west1.gcp.confluent.cloud:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 5);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "AvroProducer");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 5);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 500);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");


        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,"https://psrc-4r0k9.westus2.azure.confluent.cloud");
        props.put(AbstractKafkaAvroSerDeConfig.USER_INFO_CONFIG,"WMBVSTKCTOZVARO4:IxtnHb5mSeGiZMbrw/lOYjHDidp6oeGAfpGbRjK5o6O9GdvBo9LwgTje8YzsbCu1");
        props.put(AbstractKafkaAvroSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE,"USER_INFO");


        props.put("security.protocol","SASL_SSL");
        props.put("sasl.mechanism","PLAIN");
        props.put("ssl.endpoint.identification.algorithm","https");
        props.put("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule " +
                "required username=\"STSXDTE7WU7YGQFX\" " +
                "password=\"5R+VgUtxOlsVv4/WN0V8lju9WE+7cpKnM+RPU3cOdbnwROYRMzsoENO+P3NZRrf0\";");



        return props;
    }

    public static void main(final String[] args) {
        BasicConfigurator.configure();
        Logger.getRootLogger().setLevel(Level.INFO);

        KafkaProducer<String, ExtraInfo> producer = new KafkaProducer<String, ExtraInfo>(getConfig());

        try {

            for (int count = 0; count < recordsToGenerate; count++) {
                final ExtraInfo recordValue = new ExtraInfo(getID(), getName(), getFood(), getPC(), "");
                final ProducerRecord<String, ExtraInfo> record = new ProducerRecord<String, ExtraInfo>(TOPIC, null, recordValue);
                producer.send(record);
                //Thread.sleep(1000L);
            }

            //Tell producer to flush before exiting
            producer.flush();
            System.out.printf("Successfully produced %s%n messages to a topic called %s%n", recordsToGenerate, TOPIC);


        } catch (SerializationException a){
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
