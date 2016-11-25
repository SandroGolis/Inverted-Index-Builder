import edu.umd.cloud9.collection.wikipedia.WikipediaPage;
import edu.umd.cloud9.example.ir.BuildInvertedIndex;
import edu.umd.cloud9.io.pair.PairOfWritables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.tartarus.snowball.ext.EnglishStemmer;
import util.*;


import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.jar.JarFile;
import java.util.regex.MatchResult;

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

        job.setInputFormatClass(WikipediaPageInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(inputPath));

        job.setMapperClass(IndexMapper.class);
        job.setMapOutputKeyClass(TextPair.class);
        job.setMapOutputValueClass(TermInfo.class);


        job.setPartitionerClass(TermPartitioner.class);

        job.setGroupingComparatorClass(GroupComparator.class);

        job.setReducerClass(IndexReducer.class);
        job.setOutputKeyClass(TextPair.class);
        job.setOutputValueClass(TermInfoArray.class);

        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        return job.waitForCompletion(true);
    }

    public static class IndexMapper extends Mapper<LongWritable, WikipediaPage, TextPair, TermInfo> {
        private static final String NEW_LINES = "[\\r\\n]+";
        private static final String NOT_ALPHABETIC = "[^a-zA-Z]";
        static HashSet<String> stopWords = new HashSet<String>();

        protected void setup(Context context) throws IOException, InterruptedException {
            initializeStopWords("stop-words.txt");
        }

        @Override
        public void map(LongWritable offset, WikipediaPage page, Context context) throws IOException, InterruptedException {
            String docId = page.getDocid();
            String pageContent = page.getContent().replaceAll(NEW_LINES, " ");
            Scanner scanner = new Scanner(pageContent).useDelimiter(NOT_ALPHABETIC);
            HashMap<String, IntWritableArray> offsets = new HashMap<String, IntWritableArray>();
            EnglishStemmer stemmer = new EnglishStemmer();

            while (scanner.hasNext()) {
                String token = scanner.next().toLowerCase();
                MatchResult matchResult = scanner.match();

                // Normalize the word with porter stemmer.
                stemmer.setCurrent(token);
                if (stemmer.stem()) {
                    token = stemmer.getCurrent();
                }
                // Remove stop words
                if (stopWords.contains(token) || token.equals("")) {
                    continue;
                }
                // Update offsets HashMap
                int indexOffset = matchResult.start();
                if (offsets.containsKey(token)) {
                    offsets.get(token).add(indexOffset);
                } else {
                    IntWritableArray list = new IntWritableArray();
                    list.add(indexOffset);
                    offsets.put(token, list);
                }
            }

            for (String term : offsets.keySet()) {
                IntWritableArray indices = offsets.get(term);
                TextPair key = new TextPair(term, docId);
                TermInfo value = new TermInfo(offset.get(), indices);
                context.write(key, value);
            }
        }


        private void initializeStopWords(String fileName) throws IOException {
            Scanner stop_words_scanner = getScanner(fileName);
            while (stop_words_scanner.hasNextLine()) {
                String line = stop_words_scanner.nextLine();
                stopWords.add(line);
            }
            stop_words_scanner.close();
        }

        private Scanner getScanner(String fileName) throws IOException {
            Scanner scanner;
            ClassLoader cl = InvertedIndexBuilder.class.getClassLoader();
            String fileUrl = cl.getResource(fileName).getFile();

            if (LOCAL_MACHINE) {
                scanner = new Scanner(new File(fileUrl));
            }
            else {
                String jarUrl = fileUrl.substring(5, fileUrl.length() - fileName.length() - 2);
                JarFile jf = new JarFile(new File(jarUrl));
                scanner = new Scanner(jf.getInputStream(jf.getEntry(fileName)));
            }
            return scanner;
        }
    }

    // The partitioner makes sure all the records with the same term end up at the same reducer.
    public static class TermPartitioner extends Partitioner<TextPair, TermInfo> {
        @Override
        public int getPartition(TextPair key, TermInfo termInfo, int numPartitions) {
            return ( key.getFirst().hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }

    // The records for the reducer should be grouped only by the term part of the key
    public static class GroupComparator extends WritableComparator {
        protected GroupComparator() {
            super(TextPair.class, true);
        }
        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            return ((TextPair)w1).getFirst().compareTo(((TextPair)w2).getFirst());
        }
    }

    public static class IndexReducer extends Reducer<TextPair, TermInfo, TextPair, TermInfoArray> {

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
