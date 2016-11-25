package util;

import com.google.common.base.Joiner;
import org.apache.hadoop.io.ArrayWritable;

public class TermInfoArray extends ArrayWritable {
    public TermInfoArray() {
        super(TermInfo.class);
    }
    public TermInfoArray(TermInfo[] values) {
        super(TermInfo.class, values);
    }

    @Override
    public String toString() {
        return Joiner.on(",").join(this.get());
    }
}
