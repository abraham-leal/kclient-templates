package leal.abraham.clientsExamples;

import examples.AllSchemas;
import examples.ExtraInfo;
import examples.customerSubscription;
import examples.reorderThis;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaUtils;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.commons.lang3.SerializationException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

public class schemaRefEvolution {


    private static final String TOPIC = "mainTopic";
    public static  Random getNum = new Random();

    public static Properties getConfig (){
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "pkc-419q3.us-east4.gcp.confluent.cloud:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "AvroProducer");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 5);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 500);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");


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
        Logger.getRootLogger().setLevel(Level.DEBUG);

        KafkaProducer producer = new KafkaProducer(getConfig());

        try {
            final customerSubscription someSub = new customerSubscription(getName(), getCountry(), getSub(), 1);
            final reorderThis someTransaction = new reorderThis(getSub().toString(), "AnOrder", getName(), "someExtraField");
            final ProducerRecord<String, customerSubscription> record =
                    new ProducerRecord<>(TOPIC, null, someSub);
            final ProducerRecord<String, reorderThis> recordTransaction =
                    new ProducerRecord<>(TOPIC, null, someTransaction);
            producer.send(record);
            producer.send(recordTransaction);

            //Tell producer to flush before exiting
            producer.flush();

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

    public static String getCountry(){
        String [] x = {"USA", "Mexico", "Canada", "Norway","Argentina","England"};
        return x[getNum.nextInt(x.length)];
    }

    public static Integer getSub (){
        return getNum.nextInt(79999 - 7000) + 70000;
    }


}
