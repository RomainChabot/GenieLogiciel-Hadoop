import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Date;

/**
 * Created by rchabot on 23/01/16.
 */
public class TopKProg {

    public static class TopKMapper extends Mapper<Object, Text, Text, Text> {
        String fileName = new String();
        String unixTimestamp;
        Date startDate = new Date(2015,5,22);
        Date endDate = new Date(2015,11,25);

        @Override
        protected void setup(Context context) throws java.io.IOException, java.lang.InterruptedException
        {
            fileName = ((FileSplit) context.getInputSplit()).getPath().toString();
            unixTimestamp = FilenameUtils.removeExtension(fileName);
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        }
    }

    public static class TopKReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text ind : values)
                context.write(key, ind);
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TopKProg");
        job.setNumReduceTasks(1);
        job.setJarByClass(TopKProg.class);
        job.setMapperClass(TopKMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(TopKReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
