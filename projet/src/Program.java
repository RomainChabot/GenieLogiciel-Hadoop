import data.Action;
import data.BoundaryDate;
import data.TopKVal;
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

import java.io.File;
import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.text.DecimalFormat;
import java.util.TreeMap;

/**
 * Created by rchabot on 23/01/16.
 */
public class Program {

    public static class TopKMapper extends Mapper<Object, Text, Text, TopKVal> {
        private Text word = new Text();
        private TopKVal val = new TopKVal();
        String fileName;
        long startTimestamp;
        long endTimestamp;
        String fileExtension;
        long unixTimestamp;
        int k = 0;

        @Override
        protected void setup(Context context) throws java.io.IOException, java.lang.InterruptedException
        {
            fileName = ((FileSplit) context.getInputSplit()).getPath().toString();
            unixTimestamp = Long.parseLong(new File(FilenameUtils.removeExtension(fileName)).getName());
            fileExtension = FilenameUtils.getExtension(fileName);
            Configuration conf = context.getConfiguration();
            k = conf.getInt("k", 10);
            startTimestamp = conf.getLong("start", 0);
            endTimestamp = conf.getLong("end", 0);
            System.out.println("unixTimeStamp: "+unixTimestamp);
            System.out.println("startTimestamp: "+startTimestamp);
            System.out.println("endTimestamp: "+endTimestamp);
            System.out.println("k : "+k);
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if (fileExtension.equals("srd")) {
                Action action = Action.getFromCSV(value);
                word.set(action.getLibelle());
                val.setVal(action.getLast());
                if (unixTimestamp == startTimestamp) {
                    val.setBoundaryDate(BoundaryDate.BEGINNING);
                    System.out.println("action: "+action.getLibelle()+" (BEGINNING)");
                    context.write(word, val);
                } else if (unixTimestamp == endTimestamp){
                    val.setBoundaryDate(BoundaryDate.END);
                    System.out.println("action: "+action.getLibelle()+" (END)");
                    context.write(word, val);
                } else {
                    // On envoie rien
                }
            } else if (fileExtension.equals("ind")) {

            } else if (fileExtension.equals("dev")){

            }
        }
    }

    public static class TopKReducer extends Reducer<Text, TopKVal, IntWritable, Text> {
        private IntWritable word = new IntWritable();
        int k = 0;
        private TreeMap<Double, Text> topK = new TreeMap<Double, Text>();

        @Override
        protected void setup(Reducer.Context context) throws java.io.IOException, java.lang.InterruptedException
        {
            Configuration conf = context.getConfiguration();
            k = conf.getInt("k", 10);
        }

        @Override
        protected void reduce(Text key, Iterable<TopKVal> values, Context context) throws IOException, InterruptedException {
            double startVal = 0.0;
            double endVal = 0.0;
            for (TopKVal val : values) {
                //System.out.println("Action "+key);
                if (val.getBoundaryDate().isBeginning()) {
                    startVal = val.getVal();
                    //System.out.println("BEGINNING: startVal = "+startVal);
                }
                if (val.getBoundaryDate().isEnd()) {
                    endVal = val.getVal();
                    //System.out.println("END: endVal = "+endVal);
                }
            }
            double var = endVal - startVal;
            String mapValue = key.toString();
            topK.put(var, new Text(mapValue));
            if (topK.size() > k){
                topK.remove(topK.firstKey());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            DecimalFormat df3 = new DecimalFormat("#.###");
            int pos = topK.size();
            for (double key : topK.keySet()) {
                Text value = topK.get(key);
                word.set(pos);
                context.write(word, new Text(value+" ("+df3.format(key)+"%)"));
                pos--;
            }
        }
    }


    public static void main(String[] args) throws Exception {
        String command = "";
        String inputPath = "";
        String outputPath = "";
        int returnCode;

        if (args.length > 2) {
            command = args[0];
            inputPath = args[1];
            outputPath = args[2];
        }

        switch (command){
            case "clean":
                if (args.length != 3){
                    return;
                }
                DataCleaner cleaner = new DataCleaner(args[1], args[2]);
                cleaner.run();
                break;
            case "topk":
                if (args.length != 6) {
                    return;
                }
                Configuration conf = new Configuration();
                conf.setInt("k", Integer.parseInt(args[3]));
                conf.setLong("start", Long.parseLong(args[4]));
                conf.setLong("end", Long.parseLong(args[5]));
                Job job = configureJob(conf, inputPath, outputPath);
                configJobWithReflection(job, TopKMapper.class, null, TopKReducer.class);
                returnCode = job.waitForCompletion(true) ? 0 : 1;
                System.out.println("Finished job with result " + returnCode);
                break;
            default:
                System.out.println("Usage: commands args");
                System.out.println("commands:");
                System.out.println(" - clean [inputFolder] [outputFolder]");
                System.out.println(" - topk [inputURI] [outputURI] k [startTime] [endTime]");
        }

    }

    @SuppressWarnings("rawtypes")
    public static void configJobWithReflection(Job job, Class<? extends Mapper> mapper,
                                               Class<? extends Reducer> combiner, Class<? extends Reducer> reducer) {
        job.setMapperClass(mapper);
        if (combiner != null) {
            job.setCombinerClass(combiner);
        }
        job.setReducerClass(reducer);
        job.setMapOutputKeyClass(findSuperClassParameterType(mapper, Mapper.class, 2));
        job.setMapOutputValueClass(findSuperClassParameterType(mapper, Mapper.class, 3));
        job.setOutputKeyClass(findSuperClassParameterType(reducer, Reducer.class, 2));
        job.setOutputValueClass(findSuperClassParameterType(reducer, Reducer.class, 3));
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setInputFormatClass(TextInputFormat.class);
    }

    public static Class<?> findSuperClassParameterType(Class<?> subClass, Class<?> classOfInterest, int parameterIndex) {
        while (classOfInterest != subClass.getSuperclass()) {
            // instance.getClass() is no subclass of classOfInterest or instance is a direct instance of classOfInterest
            subClass = subClass.getSuperclass();
            if (subClass == null) throw new IllegalArgumentException();
        }
        ParameterizedType parameterizedType = (ParameterizedType) subClass.getGenericSuperclass();
        return (Class<?>) parameterizedType.getActualTypeArguments()[parameterIndex];
    }

    public static Job configureJob(Configuration conf, String inputPath, String outputPath) throws IOException {
        Job job = Job.getInstance(conf, "Program");
        job.setNumReduceTasks(1);
        job.setJarByClass(Program.class);
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        return job;
    }

}
