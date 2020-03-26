package leal.abraham.clientsExamples;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.BasicConfigurator;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class commonConsumer {

    private static final String TOPIC = "fake";

    public static Properties getConfig (){
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "TestingTopicGroup1");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "False");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        return props;
    }

    public static void main(final String[] args) throws InterruptedException {
        BasicConfigurator.configure();

        final KafkaConsumer<String, String> consumer = new KafkaConsumer<String,String>(getConfig());
        consumer.subscribe(Collections.singletonList(TOPIC));

        try {

            ConsumerRecords<String, String> records1 = consumer.poll(Duration.ofMillis(100));
            consumer.seekToBeginning(consumer.assignment());

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (final ConsumerRecord<String, String> record : records) {
                    final String key = record.key();
                    final String value = record.value();
                    final long offset = record.offset();
                    final int partition = record.partition();
                    System.out.printf("key = %s, value = %s, partition = %s, offset = %s \n", key, value, partition, offset);
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