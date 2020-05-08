package leal.abraham.clientsExamples;

import examples.ExtraInfo;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import leal.abraham.partitioners.NamePartitioner;
import org.apache.commons.lang3.SerializationException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

public class CustomPartitioningProducer {


    private static final String TOPIC = "myinternaltopic";
    private static final int recordsToGenerate = 1000000000;
    public static  Random getNum = new Random();

    public static Properties getConfig (){
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "<CCLOUD_DNS>");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 5);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "AvroProducer");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 5);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 500);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "leal.abraham.partitioners.NamePartitioner");


        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,"<CCLOUD_SR_DNS>");
        props.put(AbstractKafkaAvroSerDeConfig.USER_INFO_CONFIG,"<SR_APIKEY>:<SR_API_SECRET>");
        props.put(AbstractKafkaAvroSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE,"USER_INFO");


        props.put("security.protocol","SASL_SSL");
        props.put("sasl.mechanism","PLAIN");
        props.put("ssl.endpoint.identification.algorithm","https");
        props.put("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule " +
                "required username=\"<CCLOUD_API_KEY>>\" " +
                "password=\"<CCLOUD_API_SECRET>>\";");

        AdminClient x = AdminClient.create(props);



        return props;
    }

    public static void main(final String[] args) {
        BasicConfigurator.configure();
        Logger.getRootLogger().setLevel(Level.INFO);

        KafkaProducer<String, ExtraInfo> producer = new KafkaProducer<String, ExtraInfo>(getConfig());

        try {

            for (int count = 0; count < recordsToGenerate; count++) {
                final ExtraInfo recordValue = new ExtraInfo(getID(), getName(), getFood(), getPC(), "");
                final UUID key = UUID.randomUUID();
                final ProducerRecord<String, ExtraInfo> record = new ProducerRecord<String, ExtraInfo>(TOPIC, key.toString(), recordValue);

                // This is an example of adding a header to a record.
                record.headers().add("Who is this?", recordValue.getName().toString().getBytes());
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
