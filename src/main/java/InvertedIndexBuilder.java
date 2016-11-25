import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import util.*;
import java.io.File;


public class InvertedIndexBuilder {
    static Boolean LOCAL_MACHINE = true;

    public static void main(String[] args) throws Exception {
        if (LOCAL_MACHINE) {
            deleteDirectory(new File("C:\\Users\\Sandro\\IdeaProjects\\Query_Processing\\output"));
        }

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: InvertedIndexBuilder <in> <out>");
            System.exit(2);
        }
        conf.set("mapreduce.job.queuename", "hadoop05");

        boolean success = runMapReduceJob(conf, otherArgs[0], otherArgs[1]);
        System.exit(success ? 0 : 1);
    }

    public static boolean runMapReduceJob(Configuration conf, String inputPath, String outputPath) throws Exception {
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
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
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
