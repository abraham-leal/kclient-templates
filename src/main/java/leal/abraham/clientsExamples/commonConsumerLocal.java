package leal.abraham.clientsExamples;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.BasicConfigurator;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static java.nio.charset.StandardCharsets.UTF_8;

public class commonConsumer {

    private static final String TOPIC = "_confluent-monitoring";

    public static Properties getConfig (){
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka.superDomain.com:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "getMonitoring");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "True");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,"/Users/aleal/Documents/Git/operator-1.6/certs/myTruststore.jks");
        props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG,"mypass");
        props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG,"/Users/aleal/Documents/Git/operator-1.6/certs/testkeystore.p12");
        props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "mypass");
        props.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "PKCS12");
        props.put("confluent.monitoring.interceptor."+SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,"/Users/aleal/Documents/Git/operator-1.6/certs/myTruststore.jks");
        props.put("confluent.monitoring.interceptor."+SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG,"mypass");
        props.put("confluent.monitoring.interceptor."+SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG,"/Users/aleal/Documents/Git/operator-1.6/certs/testkeystore.p12");
        props.put("confluent.monitoring.interceptor."+SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "mypass");
        props.put("confluent.monitoring.interceptor."+SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "PKCS12");
        props.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG,"io.confluent.monitoring.clients.interceptor.MonitoringConsumerInterceptor");
        props.put("confluent.monitoring.interceptor.security.protocol","SSL");
        props.put("security.protocol","SSL");

        return props;
    }

    public static void main(final String[] args) throws InterruptedException {
        BasicConfigurator.configure();

        final KafkaConsumer<String, String> consumer = new KafkaConsumer<String,String>(getConfig());
        consumer.subscribe(Collections.singletonList(TOPIC));

        try {

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (final ConsumerRecord<String, String> record : records) {
                    final String key = record.key();
                    final String value = new String(record.value());
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