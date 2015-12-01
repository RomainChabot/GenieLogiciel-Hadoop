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
 * Created by rchabot on 29/11/15.
 */
public class HistogramProg {
    public static final String LOG_CONF = "log";
    public static final String DEC_CONF = "dec";
    public static final String HISTO_CONF = "histo_conf";

    private static class HistogramMapper extends Mapper<Object,Text,IntWritable,IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private IntWritable classe = new IntWritable();
        private String histogramConf;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            histogramConf = conf.get(HISTO_CONF);
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String tokens[] = value.toString().split(",");
            int population = 0;
            try {
                population = Integer.valueOf(tokens[4]);
            } catch (Exception e){

            }
            if (population != 0){
                if (histogramConf.equals(DEC_CONF)){
                    int pow = (int) Math.floor(Math.log10(population));
                    classe.set((population / (int) Math.pow(10,pow))*(int) Math.pow(10,pow));
                    context.write(classe, one);
                } else {
                    classe.set((int) Math.pow(10,(int) Math.floor(Math.log10(population))));
                    context.write(classe, one);
                }
            }
        }
    }

    private static class HistogramReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        private IntWritable frequency = new IntWritable();
        private IntWritable classe = new IntWritable();

        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int nbCities = 0;
            for (IntWritable value : values)
                nbCities += value.get();
            frequency.set(nbCities);
            classe.set(key.get());
            context.write(classe, frequency);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set(HISTO_CONF, LOG_CONF);
        Job job = Job.getInstance(conf, "HistogramProg");
        job.setNumReduceTasks(1);
        job.setJarByClass(HistogramProg.class);
        job.setMapperClass(HistogramMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(HistogramReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
