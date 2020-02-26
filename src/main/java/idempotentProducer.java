import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.BasicConfigurator;

import java.time.Duration;
import java.util.Properties;

public class idempotentProducer {

    private static final String TOPIC = "idempotentSingleTopic";

    public static Properties getConfig (){
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 5);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "TestingProducer");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 100);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 500);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"TRUE");
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "01ProducerTransact");

        return props;
    }

    public static void main(final String[] args) {
        BasicConfigurator.configure();

        //Start producer with configurations
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(getConfig());

        //Initialize transactional producer
        producer.initTransactions();

        try {

            // Start a finite loop, you normally will want this to be a break-bound while loop
            // However, this is a testing loop

            // Initialize transaction
            producer.beginTransaction();

            for (long i = 0; i < 30; i++) {

                //Std generation of fake key and value
                final String orderId = Long.toString(i);
                final String payment = Integer.toString(orderId.hashCode());

                // Generating record without header
                final ProducerRecord<String, String> record = new ProducerRecord<String, String>(TOPIC, orderId, payment);

                //Sending records and displaying metadata with a non-blocking callback
                //This allows to log/action on callbacks without a synchronous request
                /*producer.send(record, ((recordMetadata, e) -> {
                    System.out.println("Record was sent to topic " +
                            recordMetadata.topic() + " with offset " + recordMetadata.offset() + " in partition " + recordMetadata.partition());
                }));*/
                producer.send(record);
            }

            // Close transaction after production of this epoch of messages
            producer.commitTransaction();
        }
        catch (Exception ex){
            ex.printStackTrace();
            producer.abortTransaction();
            producer.close(Duration.ofMillis(1000));
        }

        //Tell producer to flush before exiting
        // No need to call producer.flush() as transactional producers assure flushing of uncommitted messages.
        producer.close(Duration.ofMillis(1000));
        System.out.printf("Successfully produced messages to a topic called %s%n", TOPIC);

        //Shutdown hook to assure producer close
        Runtime.getRuntime().addShutdownHook(new Thread(producer::close));

    }

}
