
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.*;

public class App {

  public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    // private Text word = new Text();
    private Set<String> dictionary = new HashSet<>(Arrays.asList(
        "John", "likes", "to", "watch", "movies",
        "also", "football", "games", "Mary", "too"));

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      // String[] line = value.toString().split("\\s+");
      String line = value.toString().toLowerCase(); // Convert to lower case
      line = line.replaceAll("[^a-z0-9 ]", " "); // Replace non-alphanumeric characters with space
      String[] words = line.split("\\s+"); // Split on whitespace
      Arrays.stream(words)
          .filter(dictionary::contains)
          .forEach(w -> {
            try {
              context.write(new Text(w), one);
            } catch (IOException | InterruptedException e) {
              throw new RuntimeException(e);
            }
          });
    }
  }

  public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      int sum = StreamSupport.stream(values.spliterator(), false)
          .mapToInt(IntWritable::get)
          .sum();
      context.write(key, new IntWritable(Math.min(sum, Integer.MAX_VALUE)));
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
    // FileInputFormat.addInputPath(job, new Path(args[0]));
    // FileInputFormat.addInputPath(job, new Path("s3://assignment2yuentatshingbilly/input.txt"));

    FileInputFormat.addInputPath(job, new Path("s3://assignment2yuentatshingbilly/testFiles"));
    FileOutputFormat.setOutputPath(job, new Path("s3://assignment2yuentatshingbilly/output"));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

}
