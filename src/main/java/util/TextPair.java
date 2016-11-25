package util;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Text;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TextPair implements WritableComparable{
    private Text first;
    private Text second;

    public TextPair() {
    }

    public TextPair(String first, String second) {
        this.first = new Text(first);
        this.second = new Text(second);
    }

    public Text getFirst() {
        return first;
    }

    public Text getSecond() {
        return second;
    }

    public int compareTo(Object obj) {
        TextPair other = (TextPair) obj;
        int cmp = getFirst().compareTo(other.getFirst());
        if (cmp != 0)
            return cmp;
        return getSecond().compareTo(other.getSecond());
    }

    public void write(DataOutput dataOutput) throws IOException {
        first.write(dataOutput);
        second.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        try {
            if (first == null)
                first = Text.class.newInstance();
            if (second == null)
                second = Text.class.newInstance();
        } catch (Exception e) {
            System.out.println("exception in TextPair.readFields: " + e.getMessage());
        }
        first.readFields(dataInput);
        second.readFields(dataInput);

    }

    @Override
    public String toString() {
        return first + "," + second;
    }
}
