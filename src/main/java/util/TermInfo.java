package util;

import com.google.common.base.Joiner;
import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TermInfo implements Writable{
    private Long offset;
    private int TF;
    private IntWritableArray indices;

    // empty constructor for reflection
    public TermInfo() {}

    public int getTF() {
        return TF;
    }

    public TermInfo(Long offset, IntWritableArray indices) {
        this.offset = offset;
        this.TF = indices.size();
        this.indices = indices;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(offset);
        dataOutput.writeInt(TF);
        indices.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        offset = dataInput.readLong();
        TF = dataInput.readInt();

        if (indices == null)
            indices = new IntWritableArray();
        indices.readFields(dataInput);
    }

    private void readFromString(String indicesStr) {
        // TODO implement
    }

    @Override
    public String toString() {
        String indicesStr = Joiner.on(",").join(indices);
        StringBuilder sb = new StringBuilder();
        sb.append("<")
                .append(offset).append(",")
                .append(TF).append(",[")
                .append(indicesStr).append("]>");
        return sb.toString();
    }
}
