package util;

import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

public class IntWritableArray extends ArrayList<Integer> implements Writable{
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.size());
        for(Integer data: this) {
            out.writeInt(data);
        }
    }

    public void readFields(DataInput in) throws IOException {
        clear();
        int size = in.readInt();
        for(int i = 0; i < size ; i++ ) {
            this.add(in.readInt());
        }
    }
}
