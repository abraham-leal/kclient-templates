package leal.abraham.SampleStreamsProcessors;

import examples.reorderThis;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;


class reorderOnCustomField implements TransformerSupplier<String, reorderThis, KeyValue<String, reorderThis>> {
    @Override
    public Transformer<String, reorderThis, KeyValue<String, reorderThis>> get() {
        return new Transformer<String, reorderThis, KeyValue<String, reorderThis>>() {
            private ProcessorContext context;
            private KeyValueStore<String, reorderThis> aggregator;


            @Override
            @SuppressWarnings("unchecked")
            public void init(ProcessorContext context) {
                this.context = context;


                // retrieve the key-value store named "reorder"
                aggregator = (KeyValueStore<String,reorderThis>) context.getStateStore("reorder");


                // schedule a punctuate() method every minute based on stream-time
                this.context.schedule(Duration.ofSeconds(60), PunctuationType.WALL_CLOCK_TIME, (timestamp) -> {
                    System.out.println("Punctuating w entry number: " + this.aggregator.approximateNumEntries() + " at " + System.currentTimeMillis());
                    KeyValueIterator<String, reorderThis> iter = this.aggregator.all();
                    ArrayList<String> sortedKeys = new ArrayList<String>();
                    while (iter.hasNext()) {
                        KeyValue<String, reorderThis> entry = iter.next();
                        sortedKeys.add(entry.key);
                    }
                    iter.close();

                    Collections.sort(sortedKeys);

                    for (String entryKey : sortedKeys) {
                        reorderThis currentValue = aggregator.get(entryKey);
                        System.out.println("Sending: " + entryKey + " " + currentValue.getName());
                        context.forward(entryKey,currentValue);
                        aggregator.delete(entryKey);
                    }
                    // commit the current processing progress
                    context.commit();
                });
            }

            @Override
            public KeyValue<String, reorderThis> transform(String key, reorderThis value) {
                System.out.println("Received: " + key + " " + value.getOrder().toString());
                this.aggregator.put(value.getTransaction().toString()+"-"+value.getOrder().toString(),value);
                return null;
            }

            @Override
            public void close() {}
        };
    }

}