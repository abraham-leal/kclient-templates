package leal.abraham.clientsExamples;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import examples.StockInfo;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import org.apache.commons.lang3.SerializationException;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.BasicConfigurator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Random;

public class fakeAVROProducerREST {


    private static final String TOPIC = "<TOPIC-TO-PRODUCE-TO>";
    private static final int numberOfAPICalls = 5;
    public static  Random getNum = new Random();

    // REST service key
    public static String key = "<REST-SECRET>";

    public static Properties getConfig (){
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "<CCLOUD-DNS>");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 5);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "AvroProducer");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 5);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5000);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");

        // NOTE: The below properties are best passed as an arguments file with proper viewing restrictions, then added
        // into the properties of the producer
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,"<CCLOUD-SR-DNS>");
        props.put(AbstractKafkaAvroSerDeConfig.USER_INFO_CONFIG,"<API-KEY>:<API-SECRET>");
        props.put(AbstractKafkaAvroSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE,"USER_INFO");
        props.put("security.protocol","SASL_SSL");
        props.put("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required username=\"<API-KEY>\" password=\"<API-SECRET>\";");
        props.put("ssl.endpoint.identification.algorithm","https");
        props.put("sasl.mechanism","PLAIN");

        return props;
    }

    public static void main(final String[] args) {
        BasicConfigurator.configure();

        KafkaProducer<String, StockInfo> producer = new KafkaProducer<String, StockInfo>(getConfig());

        try {

            for (int count = 0; count < numberOfAPICalls; count++) {
                ArrayList<StockInfo> recordsToSend = getSingleRecords(getRecords(key));

                for (StockInfo v : recordsToSend){
                    final ProducerRecord<String, StockInfo> record = new ProducerRecord<String, StockInfo>(TOPIC, null, v);
                    producer.send(record);
                }
                Thread.sleep(1000L);
            }

            //Tell producer to flush before exiting
            producer.flush();
            System.out.printf("Successfully produced messages to a topic called %s%n", TOPIC);


        } catch (SerializationException | InterruptedException | IOException a){
            a.printStackTrace();
        }

        //Shutdown hook to assure producer close
        Runtime.getRuntime().addShutdownHook(new Thread(producer::close));

    }


    // This whole method is customized to the type of data the REST service provided
    // This should be customized to obtaining bulk data from your REST Service
    public static JsonElement getRecords(String APIKEY) throws IOException {
        CloseableHttpClient client = HttpClientBuilder.create().build();

        // API of free stock data, here goes your API call
        HttpGet request = new HttpGet("https://api.worldtradingdata.com/api/v1/stock" +
                "?symbol=SNAP,TWTR,APPL,MSFT,NRZ&api_token=" + APIKEY);

        HttpResponse getData = client.execute(request);
        BufferedReader reader = new BufferedReader (new InputStreamReader(getData.getEntity().getContent()));
        String JSONString = org.apache.commons.io.IOUtils.toString(reader);

        JsonParser parser =  new JsonParser();
        JsonElement jsonElement = parser.parse(JSONString);
        JsonObject jsonObj = jsonElement.getAsJsonObject();

        return jsonObj.get("data");

    }

    // Takes the bulk data from getRecords(), utilizes AVRO object to enforce a Schema, then creates an ArrayList of the objects
    // Customize it to your use case
    public static ArrayList<StockInfo> getSingleRecords (JsonElement recordPayload){
        ArrayList<StockInfo> compliantRecords =  new ArrayList<StockInfo>();

        for (JsonElement a : recordPayload.getAsJsonArray()){
            JsonObject singleRecord = a.getAsJsonObject();
            compliantRecords.add(new StockInfo(singleRecord.get("symbol").getAsString(),singleRecord.get("name").getAsString(),singleRecord.get("currency").getAsString(),singleRecord.get("price").getAsString()));
        }

        return compliantRecords;
    }

}
