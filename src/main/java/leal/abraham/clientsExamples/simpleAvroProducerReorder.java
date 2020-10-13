package leal.abraham.clientsExamples;


import examples.customerType;
import examples.nestedAvroRecord;
import examples.simpleAvroRecord;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.SerializationException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.BasicConfigurator;
import subscription.ExtraSubDetails;

import java.util.Properties;

public class simpleAvroProducer {

    public static Properties getConfig (){
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "pkc-4yyd6.us-east1.gcp.confluent.cloud:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "simpleAvroProducer");
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,"https://psrc-4r0k9.westus2.azure.confluent.cloud");
        props.put(AbstractKafkaAvroSerDeConfig.USER_INFO_CONFIG,"WMBVSTKCTOZVARO4:IxtnHb5mSeGiZMbrw/lOYjHDidp6oeGAfpGbRjK5o6O9GdvBo9LwgTje8YzsbCu1");
        props.put(AbstractKafkaAvroSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE,"USER_INFO");
        props.put("security.protocol","SASL_SSL");
        props.put("sasl.mechanism","PLAIN");
        props.put("ssl.endpoint.identification.algorithm","https");
        props.put("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule " +
                "required username=\"CKQUW2R5XQ67WZ33\" " +
                "password=\"w6RRC0HoTFXpQYlVe/WW8nEaFiBjSmMYhtGe+919GbWfgjvTCXBQB0I61C7r697c\";");

        return props;
    }

    public static void main(final String[] args) {
        BasicConfigurator.configure();

        KafkaProducer producer = new KafkaProducer<>(getConfig());
        genericAvroProduction(producer);
        specificAvroProduction(producer);
        specificAvroProductionNested(producer);

        System.out.println("Successfully produced three records.");

        //Shutdown hook to assure producer close
        Runtime.getRuntime().addShutdownHook(new Thread(producer::close));

    }
    public static void genericAvroProduction(KafkaProducer<String,GenericRecord> reusableProducer){
        String genericSchema = "{\"type\":\"record\"," +
                "\"name\":\"myrecord\"," +
                "\"fields\":[{\"name\":\"name\",\"type\":\"string\"}]}";
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(genericSchema);
        GenericRecord avroRecord = new GenericData.Record(schema);
        avroRecord.put("name", "Abraham");

        try {
                final ProducerRecord<String, GenericRecord> record =
                        new ProducerRecord<String, GenericRecord>("avroRecordTopicGeneric", null, avroRecord);
                reusableProducer.send(record);
                reusableProducer.flush();
        } catch (SerializationException a){
            a.printStackTrace();
        }

    }
    public static void specificAvroProduction(KafkaProducer<String, simpleAvroRecord> reusableProducer){
        final simpleAvroRecord specificRecord = new simpleAvroRecord();
        specificRecord.setCountry("US");
        //specificRecord.setCustomer("NewCustomer");
        specificRecord.setSubscription(200);
        specificRecord.setTypeOfCustomer(customerType.a);
        try {
            final ProducerRecord<String, simpleAvroRecord> record =
                    new ProducerRecord<String, simpleAvroRecord>("avroRecordTopicSpecific", null, specificRecord);
            reusableProducer.send(record);
            reusableProducer.flush();
        } catch (SerializationException a){
            a.printStackTrace();
        }
    }

    public static void specificAvroProductionNested(KafkaProducer<String, nestedAvroRecord> reusableProducer){
        final nestedAvroRecord specificRecord = new nestedAvroRecord();

        ExtraSubDetails subscriptionInfo = new ExtraSubDetails();
        subscriptionInfo.setSubscriptionLevel(subscription.customerType.diamond);
        subscriptionInfo.setSubscriptionRenewalRisk(5);

        specificRecord.setCountry("US");
        specificRecord.setCustomer("NewCustomer");
        specificRecord.setSubscription(200);
        specificRecord.setTypeOfCustomer(customerType.a);
        specificRecord.setTypeOfSubscription(subscriptionInfo);
        try {
            final ProducerRecord<String, nestedAvroRecord> record =
                    new ProducerRecord<String, nestedAvroRecord>("avroRecordTopicSpecificNested", null, specificRecord);
            reusableProducer.send(record);
            reusableProducer.flush();
        } catch (SerializationException a){
            a.printStackTrace();
        }
    }

}

