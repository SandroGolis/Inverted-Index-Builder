import org.apache.hadoop.mapreduce.Reducer;
import util.TermInfo;
import util.TermInfoArray;
import util.TextPair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class IndexReducer extends Reducer<TextPair, TermInfo, TextPair, TermInfoArray> {

    public void reduce(TextPair termAndPageId, Iterable<TermInfo> values,
                       Context context) throws IOException, InterruptedException {

        List<TermInfo> list = new ArrayList<>();
        for (TermInfo termInfo : values) {
            list.add(termInfo);
        }
        Integer DF = list.size();
        TermInfoArray termsArray = new TermInfoArray(list.toArray(new TermInfo[DF]));

//            Text term = new Text(termAndPageId.getFirst());
//            IntWritable DFwritable = new IntWritable(DF);
//            PairOfWritables<Text,IntWritable> key = new PairOfWritables<>(term, DFwritable);
//
        TextPair key = new TextPair(termAndPageId.getFirst().toString(), Integer.toString(DF));
        context.write(key, termsArray);
    }
}