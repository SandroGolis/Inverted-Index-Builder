import edu.umd.cloud9.collection.wikipedia.WikipediaPage;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.tartarus.snowball.ext.EnglishStemmer;
import util.IntWritableArray;
import util.TermInfo;
import util.TextPair;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Scanner;
import java.util.jar.JarFile;
import java.util.regex.MatchResult;

/*
 * The Mapper reads WikipediaPages.
 * We consider only terms with alphabetical characters.
 * Each term is stemmed with porter stemmer and checked against the stop list words.
 * The mapper outputs aggregated values for each term as:
 * Term,docId   <Offset_1, TF, [idx1,...,idx_TF]>
 *
 */

public class IndexMapper extends Mapper<LongWritable, WikipediaPage, TextPair, TermInfo> {
    private static final String NOT_ALPHABETIC = "[^a-zA-Z]";
    private static HashSet<String> stopWords = new HashSet<>();

    protected void setup(Context context) throws IOException, InterruptedException {
        initializeStopWords("stop-words.txt");
    }

    @Override
    public void map(LongWritable offset, WikipediaPage page, Context context) throws IOException, InterruptedException {
        HashMap<String, IntWritableArray> offsets = new HashMap<>();
        EnglishStemmer stemmer = new EnglishStemmer();

        String pageContent = "";
        try {
            pageContent = page.getContent();
        }
        catch (NullPointerException e) {
            return;
        }

        Scanner scanner = new Scanner(pageContent).useDelimiter(NOT_ALPHABETIC);

        int lastStart = 0, lastEnd = 0, addition = 0;
        while (scanner.hasNext()) {
            String token = scanner.next().toLowerCase();
            MatchResult match = scanner.match();

            int start = match.start();

            if (start < lastStart) {
                // Scanner has scanned another 1K buffer
                addition += lastEnd + 1;
            }
            lastStart = start;
            lastEnd = match.end();

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
            if (offsets.containsKey(token)) {
                offsets.get(token).add(lastStart + addition);
            } else {
                IntWritableArray list = new IntWritableArray();
                list.add(lastStart + addition);
                offsets.put(token, list);
            }
        }

        String docId = page.getDocid();
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

        if (InvertedIndexBuilder.LOCAL_MACHINE) {
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
