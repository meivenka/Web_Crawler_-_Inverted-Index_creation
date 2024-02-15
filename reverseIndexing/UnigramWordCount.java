import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

  public static class Map extends Mapper<Object, Text, Text, Text> {

    private Text word = new Text();
    private Text docID = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

      StringTokenizer file = new StringTokenizer(value.toString());

      // docID is a number
      StringTokenizer itr1 = new StringTokenizer(value.toString().toLowerCase().replaceAll("[^a-zA-Z0-9]", " "));

      docID.set(file.nextToken());

      HashMap<String, Integer> wordCounting = new HashMap<String, Integer>();

      // Numbers and spedcial characters can be chucked here
      StringTokenizer itr2 = new StringTokenizer(value.toString().toLowerCase().replaceAll("[^a-zA-Z]", " "));
      String Dummy = itr2.nextToken();

      while (itr2.hasMoreTokens()) {
        word.set(itr2.nextToken());
        String currentWord = word.toString();
        if (currentWord == "") {
          continue;
        }
        if (wordCounting.containsKey(currentWord)) {
          wordCounting.put(currentWord, wordCounting.get(currentWord) + 1);
        } else {
          wordCounting.put(currentWord, 1);
        }
      }
      for (Entry<String, Integer> entry : wordCounting.entrySet()) {
        context.write(new Text(entry.getKey()), new Text(docID + ":" + entry.getValue()));
      }
    }
  }

  public static class Reduce extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      StringBuilder strings = new StringBuilder();
      for (Text docEntry : values) {
        strings.append(docEntry.toString() + ",");
      }
      context.write(key, new Text(strings.toString()));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "unigram");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}