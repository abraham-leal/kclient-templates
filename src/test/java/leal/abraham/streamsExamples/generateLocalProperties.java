package leal.abraham.streamsExamples;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

public class generateLocalProperties {

    public static Properties commonPropsString (){
        Properties generates = new Properties();
        generates.put(StreamsConfig.APPLICATION_ID_CONFIG, "testingAppLocal");
        generates.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        generates.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        generates.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        return generates;
    }

    public static Properties commonPropsStringInteger (){
        Properties generates = new Properties();
        generates.put(StreamsConfig.APPLICATION_ID_CONFIG, "testingAppLocal");
        generates.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        generates.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        generates.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());

        return generates;
    }

    public static Properties commonPropsStringLong (){
        Properties generates = new Properties();
        generates.put(StreamsConfig.APPLICATION_ID_CONFIG, "testingAppLocal");
        generates.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        generates.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        generates.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass());

        return generates;
    }

    public static Properties commonPropsIntegerString (){
        Properties generates = new Properties();
        generates.put(StreamsConfig.APPLICATION_ID_CONFIG, "testingAppLocal");
        generates.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        generates.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        generates.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        return generates;
    }

    public static Properties commonPropsLongString (){
        Properties generates = new Properties();
        generates.put(StreamsConfig.APPLICATION_ID_CONFIG, "testingAppLocal");
        generates.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        generates.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
        generates.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        return generates;
    }
}
