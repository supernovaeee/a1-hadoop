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
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

  public static class TokenizerMapper extends Mapper<Object, Text, Text, Text>{
    private Text valueText = new Text();
    private Text stateText = new Text();
    private Text cityText = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        cityText.set(fields[0]);
        stateText.set(fields[1]);
        valueText.set(fields[2]);

        context.write(stateText, valueText);
    }
    }


  public static class IntSumReducer
       extends Reducer<Text,Text,Text,Text> {
    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int total = 0;
      int max = Integer.MIN_VALUE;
      int min = Integer.MAX_VALUE;
      for (Text val : values) {
        int count = Integer.parseInt(val.toString());
        total += count;
        if (count > max){
            max = count;
        }
        if(count < min){
            min = count;
        }
      String result = String.format("%d\t%d\t%d", total, max, min);
      context.write(key, new Text(result));
    }
  }
}
}