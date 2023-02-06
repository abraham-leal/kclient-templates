package leal.abraham.clientsExamples;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.BasicConfigurator;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class commonProducer {

    private static final String TOPIC = "tst_json_payloads";

    public static Properties getConfig (){
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "pkc-v15jj.us-central1.gcp.confluent.cloud:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 5);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "TestingProducer");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 65536);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"EUHR4PPA3RQOMKER\" " +
                "password=\"NI3djkkoyCgZ5l8ZPOk0hlAOer9vzEVQql+E2Pz/V5df+5F8ygoy5LrJXuOpMoBg\";");
        props.put("sasl.mechanism", "PLAIN");
        props.put("security.protocol", "SASL_SSL");

        // Properties for auth-enabled cluster, SASL PLAIN

        //props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"test\" password=\"test123\";");
        //props.put("sasl.mechanism", "PLAIN");
        //props.put("security.protocol", "SASL_PLAINTEXT");

        return props;
    }

    public static void main(final String[] args) {
        BasicConfigurator.configure();

        //Start producer with configurations
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(getConfig());

        try {

            // Start a finite loop, you normally will want this to be a break-bound while loop
            // However, this is a testing loop
            for (long i = 0; i < 1000000000; i++) {
                //Std generation of fake key and value
                final String orderId = Long.toString(i);
                final String random = "someValue";

                // Generating record without header
                final ProducerRecord<String, String> record = new ProducerRecord<String, String>(TOPIC, orderId, random);
                record.headers().add("something", "poopHeader".getBytes(StandardCharsets.UTF_8));

                //Sending records and displaying metadata with a non-blocking callback
                //This allows to log/action on callbacks without a synchronous request
                producer.send(record, ((recordMetadata, e) -> {
                    System.out.println("Record was sent to topic " +
                            recordMetadata.topic() + " with offset " + recordMetadata.offset() + " in partition " + recordMetadata.partition());
                }));
                Thread.sleep(300);
            }

        }
        catch (Exception ex){
            ex.printStackTrace();
        }

        //Tell producer to flush before exiting
        producer.flush();
        System.out.printf("Successfully produced messages to a topic called %s%n", TOPIC);

        //Shutdown hook to assure producer close
        Runtime.getRuntime().addShutdownHook(new Thread(producer::close));

    }

}
