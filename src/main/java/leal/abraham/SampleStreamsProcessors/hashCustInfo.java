package leal.abraham.SampleStreamsProcessors;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.Set;

// This is a STATELESS Processor that hashes the value of the given message and forwards it with <Key, HashedValue>

public class hashCustInfo implements Transformer<String,String, KeyValue<String,String>>{

    private ProcessorContext context;

    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context) {
        this.context = context;
    }

    @Override
    public KeyValue<String,String> transform(String key, String value) {
        try {
            String toHash = "";
            Object fullValue = new JSONParser().parse(value);
            JSONObject fullValueJSON = (JSONObject) fullValue;
            Set<String> keys = fullValueJSON.keySet();
            for (String item : keys){
                toHash += (String)fullValueJSON.get(item) + " | ";
            }

            return new KeyValue(key, toHash.hashCode());
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }

        return null;
    }

    @Override
    public void close() {

    }

}
