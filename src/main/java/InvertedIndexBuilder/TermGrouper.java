package InvertedIndexBuilder;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import util.TextPair;

/*
 * The mapper outputs key = (term, pageId)
 * Without the InvertedIndexBuilder.TermGrouper, the reducer would have received list of values corresponding to the same key,
 * so 2 terms from different documents would have ended up in different reduce executions.
 * With the grouper, only the term matters in grouping the values.
 *
 */

public class TermGrouper extends WritableComparator {
    protected TermGrouper() {
        super(TextPair.class, true);
    }
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        return ((TextPair)w1).getFirst().compareTo(((TextPair)w2).getFirst());
    }
}