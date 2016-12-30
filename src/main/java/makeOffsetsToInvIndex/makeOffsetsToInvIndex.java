package makeOffsetsToInvIndex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.Text;
import java.io.File;

public class makeOffsetsToInvIndex {
    static Boolean LOCAL_MACHINE = false;

    public static void main(String[] args) throws Exception {
        if (LOCAL_MACHINE) {
            deleteDirectory(new File("C:\\Users\\Sandro\\IdeaProjects\\Query_Processing\\out_offsets"));
        }
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: makeOffsetsToInvIndex <in> <out>");
            System.exit(2);
        }
        conf.set("mapreduce.job.queuename", "hadoop05");

        Job job = Job.getInstance(conf, "makeOffsetsToInvIndexJob");
        job.setJarByClass(makeOffsetsToInvIndex.class);

        job.setMapperClass(OffsetMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setReducerClass(OffsetReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : 1);
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
