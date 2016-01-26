import data.Action;
import data.BoundaryDate;
import data.CorrelationKey;
import data.CorrelationValue;
import data.TopKVal;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.*;
import java.lang.reflect.ParameterizedType;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.TreeMap;

public class Program {

    public static class CleanerMapper extends Mapper<Object, BytesWritable, Text, Text>{
        private Text keyText = new Text();
        private Text valueText = new Text();
        private String fileName;
        private String fileExtension;
        private long unixTimestamp;

        @Override
        protected void setup(Context context) throws java.io.IOException, java.lang.InterruptedException
        {
            fileName = ((FileSplit) context.getInputSplit()).getPath().toString();
            unixTimestamp = Long.parseLong(new File(FilenameUtils.removeExtension(fileName)).getName());
            fileExtension = FilenameUtils.getExtension(fileName);
        }

        @Override
        protected void map(Object key, BytesWritable value, Context context) throws IOException, InterruptedException {
            InputStream is = new ByteArrayInputStream(value.getBytes());
            Document doc = Jsoup.parse(is, "UTF-8", "");
            if (fileExtension.equals("srd")) {
                Elements indexTab = doc.select("div.main-content > table > tbody > tr");
                for (Element e : indexTab) {
                    // Filename, remove unauthorized character
                    keyText.set(e.getElementsByClass("tdv-libelle").text().replaceAll(":",".")+".srd");
                    valueText.set(Action.convertToCSV(e, unixTimestamp));
                    context.write(keyText, valueText);
                }
            } else if (fileExtension.equals("ind")) {

            } else if (fileExtension.equals("dev")){

            }
            is.close();
        }
    }

    public static class CleanerReducer extends Reducer<Text, Text, NullWritable, NullWritable>{
        private String srcFolder;
        private String dstFolder;

        @Override
        protected void setup(Reducer.Context context) throws java.io.IOException, java.lang.InterruptedException
        {
            Configuration conf = context.getConfiguration();
            srcFolder = conf.get("srcFolder");
            dstFolder = conf.get("dstFolder");
            System.out.println("srcFolder: "+srcFolder);
            System.out.println("dstFolder: "+dstFolder);
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            System.out.println("Reducing action "+key);
            FileSystem fs = FileSystem.get(new Configuration());
            File tmpOutput = File.createTempFile("hadoop", "__output");
            for (Text csv : values) {
                try (PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(tmpOutput, true)))) {
                    out.println(csv.toString());
                } catch (IOException ex) {
                    //exception handling left as an exercise for the reader
                }
            }
            fs.copyFromLocalFile(new Path(tmpOutput.getPath()), new Path(dstFolder+"/data/"+key));
            tmpOutput.delete();
        }
    }

    public static class TopKMapper extends Mapper<Object, Text, Text, TopKVal> {
        private Text word = new Text();
        private TopKVal val = new TopKVal();
        private long startTimestamp;
        private long endTimestamp;
        private String fileExtension;
        private int k = 0;

        @Override
        protected void setup(Context context) throws java.io.IOException, java.lang.InterruptedException
        {
            String fileName = ((FileSplit) context.getInputSplit()).getPath().toString();
            fileExtension = FilenameUtils.getExtension(fileName);
            Configuration conf = context.getConfiguration();
            k = conf.getInt("k", 10);
            startTimestamp = conf.getLong("start", 0);
            endTimestamp = conf.getLong("end", 0);
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if (fileExtension.equals("srd")) {
                Action action = Action.getFromCSV(value.toString());
                if (action != null && action.getLast() != null) {
                    word.set(action.getLibelle());
                    val.setVal(action.getLast());
                    if (action.getTimestamp() == startTimestamp) {
                        val.setBoundaryDate(BoundaryDate.BEGINNING);
                        context.write(word, val);
                    } else if (action.getTimestamp() == endTimestamp) {
                        val.setBoundaryDate(BoundaryDate.END);
                        context.write(word, val);
                    } else {
                        // Rien
                    }
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
                if (val.getBoundaryDate().isBeginning()) {
                    startVal = val.getVal();
                }
                if (val.getBoundaryDate().isEnd()) {
                    endVal = val.getVal();
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
            ArrayList<Double> topKList = new ArrayList<>(topK.keySet());
            for (int pos=topKList.size()-1; pos >=0; pos--) {
                Double key = topKList.get(pos);
                Text value = topK.get(key);
                word.set(topKList.size()-pos);
                context.write(word, new Text(value+" ("+df3.format(key)+"%)"));
            }
        }
    }

    public static class CorrelationMapper extends Mapper<Object, BytesWritable, String, CorrelationValue>{
        private Text word = new Text();
        private TopKVal val = new TopKVal();
        private
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
        }

        @Override
        protected void map(Object key, BytesWritable value, Context context) throws IOException, InterruptedException {
            InputStream is = new ByteArrayInputStream(value.getBytes());
            BufferedReader reader = new BufferedReader(new InputStreamReader(is));
            String line;
            ArrayList<Action> actions = new ArrayList<>();
            if (fileExtension.equals("srd")) {
                while ((line = reader.readLine()) != null) {
                    Action action = Action.getFromCSV(line.toString());
                    if (action != null)
                        actions.add(action);
                }
            } else if (fileExtension.equals("ind")) {

            } else if (fileExtension.equals("dev")){

            }

            /*for (int i=0; i < actions.size(); i++){
                for (int j=i+1; j < actions.size(); j++){
                    Action action1 = actions.get(i);
                    Action action2 = actions.get(j);
                    if (!action1.equals(action2)){
                        double corrIndice = Math.abs(action1.getVar() - action2.getVar());
                        if (corrIndice < diffMax) {
                            CorrelationKey correlationKey = new CorrelationKey();
                            correlationKey.setActions(action1.getLibelle(), action2.getLibelle());

                        }
                        context.write(correlationKey, new DoubleWritable(corrIndice));
                    }
                }
            }*/
        }
    }

    public static class CorrelationReducer extends Reducer<String, CorrelationValue, String, CorrelationValue> {
        int k = 0;
        private TreeMap<CorrelationValue, String> topK = new TreeMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            k = conf.getInt("k", 10);
        }

        @Override
        protected void reduce(String key, Iterable<CorrelationValue> values, Context context) throws IOException, InterruptedException {
            System.out.println(key.toString());
            double sum = 0;
            for (DoubleWritable corrInd : values){
                sum += corrInd.get();
            }
            CorrelationKey treeValue = new CorrelationKey();
            treeValue.setActions(key.getAction1(), key.getAction2());
            topK.put(sum, treeValue);
            if (topK.size() > k)
                topK.remove(topK.lastKey());
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (double key : topK.keySet()) {
                CorrelationKey correlationKey = topK.get(key);
                context.write(correlationKey, new DoubleWritable(key));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        String command = "";
        String inputPath = "";
        String outputPath = "";
        Configuration conf;
        Job job;
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
                conf = new Configuration();
                conf.set("srcFolder", args[1]);
                conf.set("dstFolder", args[2]);
                job = configureCleanerJob(conf, inputPath, outputPath);
                configJobWithReflection(job, CleanerMapper.class, null, CleanerReducer.class);
                returnCode = job.waitForCompletion(true) ? 0 : 1;
                System.out.println("Finished job with result " + returnCode);
                break;
            case "topk":
                if (args.length != 6) {
                    return;
                }
                conf = new Configuration();
                conf.setInt("k", Integer.parseInt(args[3]));
                conf.setLong("start", Long.parseLong(args[4]));
                conf.setLong("end", Long.parseLong(args[5]));
                job = configureTopKJob(conf, inputPath, outputPath);
                configJobWithReflection(job, TopKMapper.class, null, TopKReducer.class);
                returnCode = job.waitForCompletion(true) ? 0 : 1;
                System.out.println("Finished job with result " + returnCode);
                break;
            case "correlation":
                if (args.length != 6) {
                    return;
                }
                conf = new Configuration();
                conf.setInt("k", Integer.parseInt(args[3]));
                conf.setLong("start", Long.parseLong(args[4]));
                conf.setLong("end", Long.parseLong(args[5]));
                job = configureCorrelationJob(conf, inputPath, outputPath);
                configJobWithReflection(job, CorrelationMapper.class, null, CorrelationReducer.class);
                returnCode = job.waitForCompletion(true) ? 0 : 1;
                System.out.println("Finished job with result " + returnCode);
                break;

            default:
                System.out.println("Usage: commands args");
                System.out.println("commands:");
                System.out.println(" - clean [inputFolder] [outputFolder]");
                System.out.println(" - topk [inputURI] [outputURI] k [startTime] [endTime]");
                System.out.println(" - correlation [inputURI] [outputURI] k [startTime] [endTime]");
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

    public static Job configureCleanerJob(Configuration conf, String inputPath, String outputPath) throws IOException {
        Job job = Job.getInstance(conf, "Program");
        job.setNumReduceTasks(10);
        job.setJarByClass(Program.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setInputFormatClass(WholeFileInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        return job;
    }

    public static Job configureTopKJob(Configuration conf, String inputPath, String outputPath) throws IOException {
        Job job = Job.getInstance(conf, "Program");
        job.setNumReduceTasks(1);
        job.setJarByClass(Program.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        return job;
    }

    public static Job configureCorrelationJob(Configuration conf, String inputPath, String outputPath) throws IOException {
        Job job = Job.getInstance(conf, "Program");
        job.setNumReduceTasks(1);
        job.setJarByClass(Program.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setInputFormatClass(WholeFileInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        return job;
    }


}
