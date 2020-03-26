package leal.abraham.clientsExamples;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.BasicConfigurator;

import java.util.*;

public class fakeJSONProducer {

    private static final String TOPIC = "testing-observers";
    private static final int recordsToGenerate = 10000000;

    public static Properties getConfig (){
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 5);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "TestingProducer");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 65536);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");

        return props;
    }

    public static void main(final String[] args) {
        BasicConfigurator.configure();

        //Start producer with configurations
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(getConfig());

        //get random key and values
        HashMap<String, String> payload = generateRandomJSON(getRandomCustomerIDs(recordsToGenerate));

        try {

            // Start a finite loop, you normally will want this to be a break-bound while loop
            // However, this is a testing loop
            // Lambda will iterate through the generated hashmap
            payload.forEach((k, v) ->  {

                // Generating record without header
                final ProducerRecord<String, String> record = new ProducerRecord<String, String>(TOPIC, k, v);

                //Sending records and displaying metadata with a non-blocking callback
                //This allows to log/action on callbacks without a synchronous request
                producer.send(record);

            });

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

    public static ArrayList<String> getRandomCustomerIDs (int numberOfIDsDesired){
        ArrayList<String> custID = new ArrayList<String>();
        for (int i = 0; i < numberOfIDsDesired; i++){
            custID.add(UUID.randomUUID().toString());
        }
        return custID;
    }

    public static HashMap<String, String> generateRandomJSON (ArrayList<String> UUIDs){
        HashMap<String,String> ListOfCostumers = new HashMap<String, String>();
        String[] names = {"John", "Marcus", "Susan", "Henry", "Abraham", "Valeria", "Sven", "Christoph", "Scott", "Jay", "Laurent"
        , "Genesis", "Jane", "Ava", "Paul", "Madeline"};
        String[] countries ={"United States", "Mexico", "Canada", "Peru", "Nicaragua", "Argentina", "Brazil", "London"
        , "Scotland", "France", "Spain", "Portugal", "Germany"};
        for (String id : UUIDs){
            String customerName = names[new Random().nextInt(names.length)];
            String country = countries[new Random().nextInt(countries.length)];
            String subscriptionAmount = Integer.toString(new Random().nextInt(10000000));

            String JSONpayload = "{" + "\"customer\" : \"" + customerName + "\" , \"country\" : \"" + country + "\" , \"subscription\" : \""
                    + subscriptionAmount + "\" }";

            ListOfCostumers.put(id, JSONpayload);
        }

        return ListOfCostumers;
    }


}
