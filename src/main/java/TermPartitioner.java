import org.apache.hadoop.mapreduce.Partitioner;
import util.TermInfo;
import util.TextPair;

/*
 * The partitioner makes sure all the records with the same term end up at the same reducer.
 * The mapper outputs key = (term, pageId)
 * Without the partitioner, 2 terms from different documents could have been sent to different reducers.
 * With the custom partitioner, only the term part of the key matters to which reducer it will be assigned.
 *
 */

public class TermPartitioner extends Partitioner<TextPair, TermInfo> {
    @Override
    public int getPartition(TextPair key, TermInfo termInfo, int numPartitions) {
        return ( key.getFirst().hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}