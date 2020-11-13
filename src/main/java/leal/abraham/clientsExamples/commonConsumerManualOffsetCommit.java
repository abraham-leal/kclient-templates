package leal.abraham.clientsExamples;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class commonConsumerManualOffsetCommit {
    private static Logger logging = Logger.getRootLogger();

    private static final String TOPIC = "confluent-audit-log-events";

    public static Properties getConfig (){
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka.superDomain.com:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "getMonitoring");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "True");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,"/Users/aleal/Documents/Git/operator-1.6/certs/myTruststore.jks");
        props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG,"mypass");
        props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG,"/Users/aleal/Documents/Git/operator-1.6/certs/testkeystore.p12");
        props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "mypass");
        props.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "PKCS12");
        props.put("security.protocol","SSL");

        return props;
    }

    public static void main(final String[] args) throws InterruptedException {
        BasicConfigurator.configure();
        logging.setLevel(Level.INFO);

        final KafkaConsumer<String, String> consumer = new KafkaConsumer<String,String>(getConfig());
        Map<TopicPartition, OffsetAndMetadata> myProcessedOffsets = new HashMap<>();
        consumer.subscribe(Collections.singletonList(TOPIC), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                logging.info("Revoked partition assignment, clearing offset map");
                myProcessedOffsets.clear();
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                logging.info("Received partition assignment, populating offset map");
                for (TopicPartition partition : partitions) {
                    myProcessedOffsets.put(partition, new OffsetAndMetadata(consumer.position(partition), null));
                }
            }
        });

        try {
            while (true) {
                // We are obtaining a set of records from a partition
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                for (final ConsumerRecord<String, String> record : records) {
                    // We are iterating through each record in the batch we obtained
                    final String key = record.key();
                    final String value = record.value();
                    final long offset = record.offset();
                    final int partition = record.partition();
                    System.out.printf("key = %s, value = %s, partition = %s, offset = %s \n", key, value, partition, offset);
                    // Here, we "finished processing" by printing the message, this now means we must commit the offset
                    // to assure exactly once semantics in case of failure.
                    // We commit the offset that is coming up as that is the point we would want to start from
                    if (myProcessedOffsets.containsKey(new TopicPartition(record.topic(),record.partition()))) {
                        myProcessedOffsets.put(new TopicPartition(record.topic(),record.partition()),new OffsetAndMetadata(record.offset()+1,null));
                    }
                    // We provide a callback in case of failure
                    consumer.commitAsync(myProcessedOffsets, (offsets, exception) -> {
                        // We have no tolerance for failed consumer offset commits that aren't retry-able
                        if (exception instanceof CommitFailedException) {
                            throw new KafkaException("Offset Commit was unsuccessful, this is nonrecoverable");
                        } else if (exception instanceof InterruptException){
                            throw new KafkaException("Thread was interrupted");
                        } else if (exception instanceof AuthorizationException) {
                            throw new KafkaException("Not authorized to commit offsets");
                        } else if ( exception == null ) {
                            logging.debug("Successfully committed offsets");
                        }
                        else {
                            logging.warn("Recoverable Exception, will rely on the next offset commit cycle");
                        }
                    });
                }
            }

        }
        catch (Exception e){
            e.printStackTrace();
        }
        //Shutdown hook to assure final processing and graceful shutdown when SIGTERM is received
        Runtime.getRuntime().addShutdownHook(new Thread(consumer::close));
    }
}