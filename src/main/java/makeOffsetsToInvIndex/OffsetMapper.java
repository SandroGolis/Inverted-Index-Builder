package makeOffsetsToInvIndex;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OffsetMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    @Override
    public void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException {

        String termAndDf = line.toString().split("[\t]")[0];
        String term = termAndDf.split(",")[0];

        context.write(new Text(term), offset);
    }
}