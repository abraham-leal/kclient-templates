
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.BasicConfigurator;

import java.util.Collections;
import java.util.Properties;

public class commonConsumer {

    private static final String TOPIC = "StreamEnd";

    public static Properties getConfig (){
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "TestingTopicGroup");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);

        return props;
    }

    public static void main(final String[] args) {
        BasicConfigurator.configure();

        final KafkaConsumer<String, Long> consumer = new KafkaConsumer<String,Long>(getConfig());
        consumer.subscribe(Collections.singletonList(TOPIC));

        try {

            while (true) {
                ConsumerRecords<String, Long> records = consumer.poll(100);
                for (final ConsumerRecord<String, Long> record : records) {
                    final String key = record.key();
                    final Long value = record.value();
                    final long offset = record.offset();
                    final int partition = record.partition();
                    System.out.printf("key = %s, value = %s, partition = %s, offset = %s \n", key, value, partition, offset);
                }
            }

        }
        catch (Exception e){
            e.printStackTrace();
        }
    }
}