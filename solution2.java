import java.io.IOException;
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

public class solution2 {

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "total max min");
    job.setJarByClass(solution2.class);
    job.setMapperClass(TokenizerMapper.class);
    //job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private static IntWritable valueInt = new IntWritable();
    private Text cityText = new Text();
    private Text stateText = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      String[] fields = value.toString().split(",");
      cityText.set(fields[1].trim());
      stateText.set(fields[0].trim());
      int count = Integer.parseInt(fields[2].trim());
      valueInt.set(count);      

      context.write(stateText, valueInt);
     
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,Text> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {

        int total = 0;
        int max = Integer.MIN_VALUE;
        int min = Integer.MAX_VALUE;
        for (IntWritable val : values) {
            int count = val.get();
            total += count;
            if (count > max){
                max = count;
            }
            if(count < min){
                min = count;
            }
        }
        String keyPadded = String.format("%-20s\t", key.toString());
        String result = String.format("%d\t%d\t%d", total, max, min);


      context.write(new Text(keyPadded), new Text(result));
    }
  }
}