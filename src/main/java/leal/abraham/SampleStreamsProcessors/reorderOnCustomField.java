package leal.abraham.SampleStreamsProcessors;

import examples.reorderThis;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;


class reorderOnCustomField implements TransformerSupplier<String, reorderThis, KeyValue<String, reorderThis>> {
    @Override
    public Transformer<String, reorderThis, KeyValue<String, reorderThis>> get() {
        return new Transformer<String, reorderThis, KeyValue<String, reorderThis>>() {
            private ProcessorContext context;
            private KeyValueStore<String, reorderThis> aggregator;
            private ArrayList<String> keyTracker;


            @Override
            @SuppressWarnings("unchecked")
            public void init(ProcessorContext context) {
                // Retrieve the key-value store named "reorder" from the topology
                this.aggregator = (KeyValueStore<String,reorderThis>) context.getStateStore("reorder");
                // Get the current context
                this.context = context;
                // Track keys being registered in the underlying store
                this.keyTracker = new ArrayList<>();

                /*
                 * Schedule a punctuate() method every minute based on wall-clock-time.
                 * This will run according to the delay given and in accordance to poll() calls
                 * It can also do a punctuation call in StreamTime, which does not advance unless new records are
                 * present in source partitions
                 */
                this.context.schedule(Duration.ofSeconds(60), PunctuationType.WALL_CLOCK_TIME, (timestamp) -> {
                    // Sort the keys received
                    Collections.sort(keyTracker);

                    Iterator<String> itr = keyTracker.iterator();
                    while (itr.hasNext()) {
                        String currentMod = itr.next();
                        // Get an entry
                        reorderThis currentValue = aggregator.get(currentMod);
                        // Send entry forward into the topic
                        context.forward(currentMod,currentValue);
                        // Delete entry from persistent store and remove from keyTracker
                        aggregator.delete(currentMod);
                        itr.remove();
                    }
                    // commit the current processing progress
                    context.commit();
                });
            }

            @Override
            public KeyValue<String, reorderThis> transform(String key, reorderThis value) {
                // Establish a compound key on which to sort with
                String compoundKey = value.getTransaction().toString()+"-"+value.getOrder().toString();
                // Add the record to the underlying persistent store and our keyTracker
                this.keyTracker.add(compoundKey);
                this.aggregator.put(compoundKey,value);
                // Return null, since we do not want to act upon the record immediately
                return null;
            }

            @Override
            public void close() {} // Nothing to close
        };
    }

}