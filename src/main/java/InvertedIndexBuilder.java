import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import util.*;
import java.io.File;


/*
 * The inverted index list will be sorted alphabetically as a side effect of mapreduce.
 * In addition to that, we want each record in the inverted index to be sorted by pageId.
 * So, for each term, the list of documents it appears in them will be sorted from earliest to latest.
 * This sorting will make more efficient query processing.
 * For this reason, the mapper outputs the key value with redundant pageId. KEY=(Term,PageId)
 * Due to mapreduces internal sorting, this will make the list of pages to appear sorted.
 *
 * The record in the final inverted index will be of the following form:
 *
 * Term,DF   <Offset_1, TF1, [idx_1,...,idx_TF1]> ,..., <Offset_DF, TF2, [idx_1,...,idx_TF2]>
 *
 * Term      - a stemmed token using porter stemmer
 * DF        - document frequency of this term
 * Offset_1  - the offset till the beginning of the first page
 * TF1       - term frequency in the page that starts at Offset_1
 * idx_      - indices of the beginning of the term inside the page
 *
*/

public class InvertedIndexBuilder {
    static Boolean LOCAL_MACHINE = false;

    public static void main(String[] args) throws Exception {
        if (LOCAL_MACHINE) {
            deleteDirectory(new File("C:\\Users\\Sandro\\IdeaProjects\\Query_Processing\\output"));
        }

        Configuration conf = new Configuration();
        conf.set("mapreduce.job.queuename", "hadoop05");

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 3) {
            System.err.println("Usage: InvertedIndexBuilder <in> <out> <0 (Text), 1 (SeqFileFormat)>");
            System.exit(2);
        }

        String input = otherArgs[0];
        String output = otherArgs[1];
        boolean isSeqFileFormat = Integer.parseInt(otherArgs[2]) == 1;

        boolean success = runMapReduceJob(conf, input, output, isSeqFileFormat);
        System.exit(success ? 0 : 1);
    }

    public static boolean runMapReduceJob(Configuration conf, String inputPath, String outputPath, boolean isSeqFileFormat) throws Exception {
        Job job = Job.getInstance(conf, "InvertedIndexBuilderJob");
        job.setJarByClass(InvertedIndexBuilder.class);

        // input
        job.setInputFormatClass(WikipediaPageInputFormat.class);
        WikipediaPageInputFormat.addInputPath(job, new Path(inputPath));

        // mapper
        job.setMapperClass(IndexMapper.class);
        job.setMapOutputKeyClass(TextPair.class);
        job.setMapOutputValueClass(TermInfo.class);

        //job.setNumReduceTasks(0);

        // partitioner
        job.setPartitionerClass(TermPartitioner.class);

        // grouper
        job.setGroupingComparatorClass(TermGrouper.class);

        // reducer
        job.setReducerClass(IndexReducer.class);
        job.setOutputKeyClass(TextPair.class);
        job.setOutputValueClass(TermInfoArray.class);

        // output
        if (isSeqFileFormat) {
            job.setOutputFormatClass(SequenceFileOutputFormat.class);
            SequenceFileOutputFormat.setOutputPath(job, new Path(outputPath));
        } else {
            FileOutputFormat.setOutputPath(job, new Path(outputPath));
        }

        return job.waitForCompletion(true);
    }

    private static boolean deleteDirectory(File directory) {
        if (directory.exists()) {
            File[] files = directory.listFiles();
            if (null != files) {
                for (File file : files) {
                    if (file.isDirectory()) {
                        deleteDirectory(file);
                    } else {
                        file.delete();
                    }
                }
            }
        }
        return (directory.delete());
    }
}
