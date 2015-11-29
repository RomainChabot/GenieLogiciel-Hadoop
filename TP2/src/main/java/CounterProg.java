import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Created by rchabot on 04/11/15.
 */

public class CounterProg {
    public static final String WCP_GROUP = "WCP";

    private static class CounterMapper extends Mapper<Object, Text, Text, IntWritable> {
        private IntWritable pop = new IntWritable();
        private Text word = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String tokens[] = value.toString().split(",");
            String city = tokens[1];
            int population = 0;
            try {
                population = Integer.valueOf(tokens[4]);
            } catch (Exception e){
                context.getCounter(WCP_GROUP, WCPCounters.NB_INVALID_CITIES.toString()).increment(1);
            }
            if (population != 0){
                word.set(city);
                pop.set(population);
                context.getCounter(WCP_GROUP, WCPCounters.NB_POP.toString()).increment(1);
                context.getCounter(WCP_GROUP, WCPCounters.TOT_POP.toString()).increment(population);
                context.write(word, pop);
            }
        }
    }

    private static class CounterReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
        private final static IntWritable pop = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int cityPop = 0;
            for (IntWritable value : values)
                cityPop += value.get();
            pop.set(cityPop);
            context.getCounter(WCP_GROUP, WCPCounters.NB_CITIES.toString()).increment(1);
            context.write(key, pop);
        }

    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "CounterProg");
        job.setNumReduceTasks(1);
        job.setJarByClass(CounterProg.class);
        job.setMapperClass(CounterMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(CounterReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
