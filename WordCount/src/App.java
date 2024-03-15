import java.io.IOException;
import java.util.*;
import java.util.stream.StreamSupport;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class App {

  public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private final static String[] top100Word = { "the", "be", "to", "of", "and", "a",
        "in", "that", "have", "i", "it", "for", "not", "on", "with", "he", "as", "you", "do",
        "at", "this", "but", "his", "by", "from", "they", "we", "say", "her", "she", "or", "an",
        "will", "my", "one", "all", "would", "there", "their", "what", "so", "up", "out", "if",
        "about", "who", "get", "which", "go", "me", "when", "make", "can", "like", "time",
        "no", "just", "him", "know", "take", "people", "into", "year", "your", "good",
        "some", "could", "them", "see", "other", "than", "then", "now", "look", "only",
        "come", "its", "over", "think", "also", "back", "after", "use", "two", "how", "our",
        "work", "first", "well", "way", "even", "new", "want", "because", "any", "these",
        "give", "day", "most", "us" };
    private Set<String> dictionary = new HashSet<>(Arrays.asList(top100Word));

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      Path filePath = ((FileSplit) context.getInputSplit()).getPath();
      String fileName = filePath.getName();
      // System.out.println(fileName);

      String line = value.toString().toLowerCase(); // Convert to lower case
      line = line.replaceAll("[^a-z0-9 ]", " "); // Replace non-alphanumeric characters with space
      String[] words = line.split("\\s+"); // Split on whitespace
      Arrays.stream(words)
          .filter(dictionary::contains)
          .forEach(w -> {
            try {
              context.write(new Text(w + "@" + fileName), one);
            } catch (IOException | InterruptedException e) {
              throw new RuntimeException(e);
            }
          });
    }
  }

  public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    private MultipleOutputs<Text, IntWritable> multipleOutputs;

    @Override
    public void setup(Context context) {
      multipleOutputs = new MultipleOutputs<>(context);
    }

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      int sum = StreamSupport.stream(values.spliterator(), false)
          .mapToInt(IntWritable::get)
          .sum();
      String[] wordAndFileName = key.toString().split("@");
      String word = wordAndFileName[0];
      String fileName = wordAndFileName[1];
      multipleOutputs.write(key, new IntWritable(Math.min(sum, Integer.MAX_VALUE)), fileName);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      multipleOutputs.close();
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "bagofwords");
    job.setJarByClass(App.class);
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    job.setNumReduceTasks(1);
    FileInputFormat.addInputPath(job, new Path("s3://assignment2yuentatshingbilly/testFiles"));
    FileOutputFormat.setOutputPath(job, new Path("s3://assignment2yuentatshingbilly/output"));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
