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

public class NumAnalysis {

    public static class RegionMapper extends Mapper<Object, Text, IntWritable, RegionSummary> {
        public int nb_step = 0;
        public void setup(Context context) {
            Configuration conf = context.getConfiguration();
            nb_step = conf.getInt("steps", 10);
        }
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String tokens[] = value.toString().split(",");
            if (tokens.length < 7 || tokens[4].length()==0)
                return;
            else {

                RegionSummary regionStats;
                String city;
                int region;
                int pop;
                double latitude;
                double longitude;

                try {
                    city = tokens[1];
                    region = Integer.parseInt(tokens[3]);
                    pop = Integer.parseInt(tokens[4]);
                    latitude = Double.parseDouble(tokens[5]);
                    longitude = Double.parseDouble(tokens[6]);

                    Box box = new Box(latitude, latitude, longitude, longitude);
                    regionStats = new RegionSummary(city, pop, pop, 1, box);
                    context.write(new IntWritable(region), regionStats);
                } catch (IncoherentLatLongException e) {
                    e.printStackTrace();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
    public static class RegionCombiner extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        public void reduce(IntWritable key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
        //TODO: a changer
    }
    public static class RegionReducer extends Reducer<IntWritable, IntWritable, Text, IntWritable> {
        public int nb_step = 0;
        public void setup(Context context) {
            Configuration conf = context.getConfiguration();
            nb_step = conf.getInt("steps", 10);
        }
        public void reduce(IntWritable key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            int val = (int)Math.pow(10, key.get()/(double)nb_step);
            StringBuffer b = new StringBuffer();
            b.append(val);
            context.write(new Text(b.toString()), new IntWritable(sum));
        }
        //TODO: a changer
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Clean population");
        job.setJarByClass(NumAnalysis.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setInputFormatClass(TextInputFormat.class);


        String commande = "";
        if (args.length>2) {
            commande = args[0];
            FileInputFormat.addInputPath(job, new Path(args[1]));
            FileOutputFormat.setOutputPath(job, new Path(args[2]));
        }
        int returnCode = 0;
        switch (commande) {
            case "clean":
                job.setMapperClass(RegionMapper.class);
                job.setReducerClass(RegionReducer.class);
                returnCode = job.waitForCompletion(true) ? 0 : 1;
                break;
        }
        System.out.println("Usage: commands args");
        System.out.println("commands:");
        System.out.println(" - clean [inputURI] [outputURI]");
        System.out.println(" - histo [inputURI] [outputURI] [NBSTEPS");
        returnCode = 1;
        System.exit(returnCode);
    }
}
