/**
 * Created by rchabot on 18/10/15.
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.FileSystem;

import java.io.*;
import java.net.URI;

public class CopyHDFS {
    public static class MonProg extends Configured implements Tool {
        public int run(String[] args) throws Exception {
            String localSrc=args[0];
            String dst=args[1];
            InputStream in=new BufferedInputStream(new FileInputStream(localSrc));
            Configuration conf=new Configuration();
            FileSystem fs = FileSystem.get(URI.create(dst), conf);
            OutputStream out=fs.create(new Path(dst),new Progressable(){
                public void progress(){
                    System.out.print("*");
                }
            });
            IOUtils.copyBytes(in, out, 4096, true);

            return 0;
        }
    }
    public static void main( String[] args ) throws Exception {
        int returnCode = ToolRunner.run(new CopyHDFS.MonProg(), args);
        System.exit(returnCode);
    }
}
