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

    private Text bigram = new Text();
    private Text docID = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      StringTokenizer itr1 = new StringTokenizer(value.toString());

      docID.set(itr1.nextToken());
      
      StringTokenizer itr = new StringTokenizer(value.toString().toLowerCase().replaceAll("[^a-zA-Z]", " "));
      String dummyWord = itr.nextToken();
      String Word1 = "";
      if (itr.hasMoreTokens()) {
        Word1 = itr.nextToken();
      }
      while (itr.hasMoreTokens()) {
        String Word2 = itr.nextToken();
        if (Word2 == "") {
          continue;
        }
        bigram.set(Word1 + " " + Word2);
        // Next Iteration
        Word1 = Word2;
        context.write(bigram, docID);
      }
    }
  }

  public static class Reduce extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      StringBuilder strings = new StringBuilder();
      int wordcount = 0;
      HashMap<String, Integer> wordCounting = new HashMap<String, Integer>();
      for (Text val : values) {
        String docID = val.toString();
        if (wordCounting.containsKey(docID)) {
          int newCount = wordCounting.get(docID) + 1;
          wordCounting.put(docID, newCount);
        } else {
          wordCounting.put(docID, 1);
        }
      }
      for (Entry<String, Integer> entry : wordCounting.entrySet()) {
        strings.append(entry.getKey() + ":" + entry.getValue() + ",");
        wordcount++;
      }

      context.write(key, new Text(strings.toString()));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "bigram");
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