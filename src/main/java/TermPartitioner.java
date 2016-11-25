import org.apache.hadoop.mapreduce.Partitioner;
import util.TermInfo;
import util.TextPair;

// The partitioner makes sure all the records with the same term end up at the same reducer.
public class TermPartitioner extends Partitioner<TextPair, TermInfo> {
    @Override
    public int getPartition(TextPair key, TermInfo termInfo, int numPartitions) {
        return ( key.getFirst().hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}