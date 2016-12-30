package makeOffsetsToInvIndex;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OffsetReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    LongWritable valueOut = new LongWritable();

    public void reduce(Text term, Iterable<LongWritable> values,
                       Context context) throws IOException, InterruptedException {
        for (LongWritable offset : values) {
            Long offsetVal = offset.get();
            valueOut.set(offsetVal);
            break;
        }
        context.write(term, valueOut);
    }
}