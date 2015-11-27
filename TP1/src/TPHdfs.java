/**
 * @author David Auber 
 * Maître de conférencces HDR
 * LaBRI: Université de Bordeaux
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.*;
import java.net.URI;
import java.util.Random;


public class TPHdfs {

	private static class FilteringMapper extends Mapper<Object, Text, Text, BooleanWritable> {

	}

	public static class CopyFromLocal extends Configured implements Tool {

		public int run(String[] args) throws Exception {
			if (args.length < 3) {
				System.err.println("HdfsWriter [local input path] [hdfs output path]");
				return 1;
			}

			String localInputPath = args[1];
			URI uri = new URI(args[2]);
			uri = uri.normalize();

			Configuration conf = getConf();
			FileSystem fs = FileSystem.get(uri, conf);
			Path outputPath = new Path(uri.getPath()); 

			if (fs.exists(outputPath)) {
				System.err.println("output path exists");
				return 1;
			}

			OutputStream os = fs.create(outputPath);
			InputStream is = new BufferedInputStream(new FileInputStream(localInputPath));
			IOUtils.copyBytes(is, os, conf);

			os.close();
			is.close();
			return 0;
		}
	}

	public static class MergeFromLocal extends Configured implements Tool {

		public int run(String[] args) throws Exception {
			if (args.length < 3) {
				System.err.println("HdfsWriter [local input paths] [hdfs output path]");
				return 1;
			}

			URI uri = new URI(args[args.length - 1]);
			uri = uri.normalize();

			Configuration conf = getConf();
			FileSystem fs = FileSystem.get(uri, conf, "haddop");
			Path outputPath = new Path(uri.getPath()); 

			if (fs.exists(outputPath)) {
				System.err.println("output path exists");
				return 1;
			}

			OutputStream os = fs.create(outputPath);
			for (int i=1; i < args.length - 1; ++i) {
				InputStream is = new BufferedInputStream(new FileInputStream(args[i]));
				IOUtils.copyBytes(is, os, conf, false);
				is.close();
			}
			
			os.close();
			return 0;
		}
	}

	public static class GenerateWords extends Configured implements Tool {

		String alphabet[] = {"ca","co","ce","la", "le","lu","la","il","el","be","par","pir","por","ou","da","de","di","do","du"};
		
		public int run(String[] args) throws Exception {
			if (args.length < 4) {
				System.err.println("generateWords [nb syl] [nb words] [URI]");
				return 1;
			}

			URI uri = new URI(args[args.length - 1]);
			uri = uri.normalize();
			Configuration conf = getConf();
			FileSystem fs = FileSystem.get(uri, conf);
			Path outputPath = new Path(uri.getPath()); 
			if (fs.exists(outputPath)) {
				System.err.println("output path exists");
				return 1;
			}
			OutputStream os = fs.create(outputPath);
			PrintStream ps = new PrintStream(os); 
			Random rand = new Random();
			final int nbwords = Integer.parseInt(args[2]);
			final int maxSyl = Integer.parseInt(args[1]);
			
			for ( int i=0; i<nbwords; ++i) {
				StringBuffer tmp = new StringBuffer();
				for (int nbSyllabe=0; nbSyllabe<maxSyl; ++nbSyllabe) {
					tmp.append(alphabet[rand.nextInt(alphabet.length)]);
				}
				tmp.append("\n");
				ps.print(tmp.toString());
			}			
			os.close();
			return 0;
		}
	}
	
	public static void main( String[] args ) throws Exception {
		String commande = "";
		if (args.length>0)
			commande = args[0];
		int returnCode = 0;
		switch (commande) {
		case "copyFromLocal":
			returnCode = ToolRunner.run(new TPHdfs.CopyFromLocal(),  args);
			break;
		case "mergeFromLocal":
			returnCode = ToolRunner.run(new TPHdfs.MergeFromLocal(), args);
			break;
		case "generateWords":
			returnCode = ToolRunner.run(new TPHdfs.GenerateWords(),  args);
			break;
		default:
			System.out.println("Usage: commands args");
			System.out.println("commands:");
			System.out.println(" - copyFromLocal  [File]   [URI]");
			System.out.println(" - mergeFromLocal [Files]  [URI]");
			System.out.println(" - generateWords  [NB_SYL] [NB_WORD] [URI]");
			returnCode = 1;
		}
		System.exit(returnCode);
	}
}