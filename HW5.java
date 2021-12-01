import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.util.HashSet;
import java.util.Arrays;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.StringTokenizer;


public class HW5
{
    public static class WordCountMapper extends Mapper<Object, Text, Text, IntWritable>
    {
        private final static IntWritable one = new IntWritable(1);

        private Text word = new Text();

        /*
         List of stop words. Stored in a hash set for fast lookups. I could have read these in via a text file but thought it would be
         better to have everything in one java file. List taken from https://www.ranks.nl/stopwords
        */ 
        private HashSet<String> stopWords = new HashSet<String>(Arrays.asList("a", "about", "above", "after",
        "again", "against", "all", "am", "an", "and", "any", "are", "aren't", "as", "at", "be", "because",
        "been", "before", "being", "below", "between", "both", "but", "by", "can't", "cannot", "could",
        "couldn't", "did", "didn't", "do", "does", "doesn't", "doing", "don't", "down", "during", "each",
        "few", "for", "from", "further", "had", "hadn't", "has", "hasn't", "have", "haven't", "having", "he",
        "he'd", "he'll", "he's", "her", "here", "here's", "hers", "herself", "him", "himself", "his", "how",
        "how's", "i", "i'd", "i'll", "i'm", "i've", "if", "in", "into", "is", "isn't", "it", "it's", "its",
        "itself", "let's", "me", "more", "most", "mustn't", "my", "myself", "no", "nor", "not", "of", "off",
        "on", "once", "only", "or", "other", "ought", "our", "ours", "ourselves", "out", "over", "own", "same", 
        "shan't", "she", "she'd", "she'll", "she's", "should", "shouldn't", "so", "some", "such", "than",
        "that", "that's", "the", "their", "theirs", "them", "themselves", "then", "there", "there's", "these",
        "they", "they'd", "they'll", "they're", "they've", "this", "those", "through", "to", "too", "under",
        "until", "up", "very", "was", "wasn't", "we", "we'd", "we'll", "we're", "we've", "were", "weren't",
        "what", "what's", "when", "when's", "where", "where's", "which", "while", "who", "who's", "whom",
        "why", "why's", "with", "won't", "would", "wouldn't", "you", "you'd", "you'll", "you're",
        "you've", "your", "yours", "yourself", "yourselves"));

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException
        {
            String valueString = value.toString();
            
            // Tokenize input by hyphen and space
            StringTokenizer itr = new StringTokenizer(valueString, " -");
            
            // Loop through all words in the line
            while (itr.hasMoreTokens()) 
            {
                // Remove special characters, tabs, and newlines, and make everyting lowercase
                word.set(itr.nextToken().replaceAll("[^a-zA-Z]", "").replaceAll("[\\n\\t ]", "").toLowerCase());

                // Make sure string isn't empty or in the stop list before counting it
                if(word.toString() != "" && !word.toString().isEmpty() && !stopWords.contains(word.toString()))
                {
                    // Write word with its associated file to the reducer
                    context.write(word, one);
                }
            }
        }
    }
    
    public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>
    {
        private IntWritable wordcount = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
        {
            int total = 0;

            // Sum up values for each word
            for(IntWritable value : values)
            {
                total += value.get();
            }

            wordcount.set(total);

            context.write(key, wordcount);
        }
    }


    public static class TopNMapper extends Mapper<Object, Text, Text, IntWritable>
    {
        private int n;
        private TreeMap<Integer, String> wordList;

        public void setup(Context context)
        {
            // Get value of n from configuration and initialize a treemap to store the top n words for the mapper
            n = Integer.parseInt(context.getConfiguration().get("N"));
            wordList = new TreeMap<Integer, String>();
        }

        public void map(Object key, Text value, Context context)
        {
            // Split word and wordcount
            String[] line = value.toString().split("\t");

            // add each word and its count to the treemap
            wordList.put(Integer.valueOf(line[1]), line[0]);

            // if the tree map is full, remove the first key which is the smallest count
            if (wordList.size() > n)
            {
                wordList.remove(wordList.firstKey());
            }
        }

        public void cleanup(Context context) throws IOException, InterruptedException
        {
            // Swap keys before writing, so format will be word, count
            for (Map.Entry<Integer, String> entry : wordList.entrySet())
            {
                context.write(new Text(entry.getValue()), new IntWritable(entry.getKey()));
            }
        }
    }

    /* input:  <word, wordcount> (with the local topN words)
     * output: <wordcount, word> (with the global topN words)
     */
    public static class TopNReducer extends Reducer<Text, IntWritable, IntWritable, Text>
    {
        private int n;
        private TreeMap<Integer, String> wordList;

        public void setup(Context context)
        {
            // Get n value from context and initialize the final word count tree map
            n = Integer.parseInt(context.getConfiguration().get("N"));
            wordList = new TreeMap<Integer, String>();
        }

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
        {
            int wordcount = 0;

            // Get the value for each word
            for(IntWritable value : values)
            {
                wordcount = value.get();
            }

            // Add the count to our word list
            wordList.put(wordcount, key.toString());

            // Remove lowest entry in tree map if it is full
            if (wordList.size() > n)
            {
                wordList.remove(wordList.firstKey());
            }
        }

        public void cleanup(Context context) throws IOException, InterruptedException
        {
            // Write output to file in form of word, count
            for (Map.Entry<Integer, String> entry : wordList.entrySet())
            {
                context.write(new IntWritable(entry.getKey()), new Text(entry.getValue()));
            }
        }
    }


    public static void main(String[] args) throws Exception
    {
        // Create config and set a public variable for n to be 5
        Configuration config = new Configuration();
        config.set("N", "5");
        // Establis path for output of wordcount job and input of top n job
        Path wordcountOutput = new Path("wordcountOutput");

        // Create and start wordcount job
        Job wordCountJob = Job.getInstance(config, "WordCount");
        wordCountJob.setJarByClass(HW5.class);
        wordCountJob.setMapperClass(WordCountMapper.class);
        wordCountJob.setReducerClass(WordCountReducer.class);
        wordCountJob.setMapOutputKeyClass(Text.class);
        wordCountJob.setMapOutputValueClass(IntWritable.class);
        wordCountJob.setOutputKeyClass(Text.class);
        wordCountJob.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(wordCountJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(wordCountJob, wordcountOutput);
        wordCountJob.waitForCompletion(true);
        
        // Create and start top n job
        Path output = new Path(args[1]);
        Job topNJob = Job.getInstance(config, "TopN");
        topNJob.setJarByClass(HW5.class);
        topNJob.setMapperClass(TopNMapper.class);
        topNJob.setReducerClass(TopNReducer.class);
        topNJob.setMapOutputKeyClass(Text.class);
        topNJob.setMapOutputValueClass(IntWritable.class);
        topNJob.setOutputKeyClass(IntWritable.class);
        topNJob.setOutputValueClass(Text.class);
        topNJob.setNumReduceTasks(1);
        FileInputFormat.addInputPath(topNJob, wordcountOutput);
        FileOutputFormat.setOutputPath(topNJob, output);
        topNJob.waitForCompletion(true);
    }
}