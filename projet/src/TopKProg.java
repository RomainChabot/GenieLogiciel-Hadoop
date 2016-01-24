import data.Indice;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;

/**
 * Created by rchabot on 23/01/16.
 */
public class TopKProg {

    public static class TopKMapper extends Mapper<Object, Text, Text, Text> {
        private Text word = new Text("a");
        String fileName = new String();
        protected void setup(Context context) throws java.io.IOException, java.lang.InterruptedException
        {
            fileName = ((FileSplit) context.getInputSplit()).getPath().toString();
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println(fileName + " " + key);
            Document doc = Jsoup.parse(value.toString());
            Elements indexTab = doc.select("tr[href^=/cours.phtml?symbole]");
            for (Element e : indexTab)
            {
                Indice ind = new Indice();
                ind.setLibelle(e.getElementsByClass("tdv-libelle").text());
                ind.setLast(Double.valueOf(e.getElementsByClass("tdv-last").text()));
                ind.setVar(Double.valueOf(e.getElementsByClass("tdv-var").text()));
                ind.setOpen(Double.valueOf(e.getElementsByClass("tdv-open").text()));
                ind.setHigh(Double.valueOf(e.getElementsByClass("tdv-high").text()));
                ind.setLow(Double.valueOf(e.getElementsByClass("tdv-low").text()));
                ind.setPrevClose(Double.valueOf(e.getElementsByClass("tdv-prev_close").text()));
                ind.setVarAn(Double.valueOf(e.getElementsByClass("tdv-var_an").text()));
                System.out.println("coucou "+ind.getLibelle());
                context.write(word, new Text(ind.getLibelle()));
            }
        }
    }

    public static class TopKReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text ind : values)
                context.write(key, ind);
        }
    }

    public class WholeInputFileFormat extends FileInputFormat{

        @Override
        protected boolean isSplitable(JobContext context, Path filename) {
            return false;
        }

        @Override
        public RecordReader createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            return null;
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TopKProg");
        job.setNumReduceTasks(1);
        job.setJarByClass(TopKProg.class);
        job.setMapperClass(TopKMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(TopKReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setInputFormatClass(NLineInputFormat.class);
        NLineInputFormat.addInputPath(job, new Path(args[0]));
        NLineInputFormat.setInputDirRecursive(job, true);
        NLineInputFormat.setNumLinesPerSplit(job, 100000);

        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
