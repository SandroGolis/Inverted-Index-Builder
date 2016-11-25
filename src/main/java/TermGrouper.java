import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import util.TextPair;

// The records for the reducer should be grouped only by the term part of the key
public class TermGrouper extends WritableComparator {
    protected TermGrouper() {
        super(TextPair.class, true);
    }
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        return ((TextPair)w1).getFirst().compareTo(((TextPair)w2).getFirst());
    }
}