package leal.abraham.clientsExamples;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.BasicConfigurator;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class commonConsumerManualOffsetCommit {

    private static final String TOPIC = "newTopic";

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

        final KafkaConsumer<String, String> consumer = new KafkaConsumer<String,String>(getConfig());
        consumer.subscribe(Collections.singletonList(TOPIC));

        try {

            while (true) {
                // We are obtaining a set of records from a partition
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                Map<TopicPartition, OffsetAndMetadata> myProcessedOffsets = new HashMap<>();
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
                    myProcessedOffsets.put(new TopicPartition(record.topic(),record.partition()),new OffsetAndMetadata(record.offset()+1,null));
                    // We provide a callback in case of failure
                    consumer.commitAsync(myProcessedOffsets, (offsets, exception) -> {
                        // We have no tolerance for failed consumer offset commits
                        throw new KafkaException("Offset Commit was unsuccessful");
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