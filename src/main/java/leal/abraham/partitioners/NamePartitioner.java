package leal.abraham.partitioners;

import examples.ExtraInfo;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;

public class NamePartitioner implements Partitioner {


    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        // This partitioner assures the assignment of a partition by the name field of the value of a record
        // If the value is not an instance of the expected avro java object, it assigns a partition based on the
        // simple hash of a key (NOTE: This is not the default murmur2 algorithm.

        int assignedPartition = 0;

        int totalPartitions = cluster.partitionsForTopic(topic).size();

        if (value instanceof ExtraInfo) {
            ExtraInfo record = (ExtraInfo) value;
            assignedPartition = record.getName().length()%totalPartitions;
        }else {
            assignedPartition = key.hashCode()%totalPartitions;
        }
        return assignedPartition;
    }

    @Override
    public void close() {
        // Nothing to close

    }

    @Override
    public void configure(Map<String, ?> configs) {
        // No configs needed for partitioning
    }
}
