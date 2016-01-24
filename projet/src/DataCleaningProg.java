import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.*;

/**
 * Created by rchabot on 24/01/16.
 */
public class DataCleaningProg {

    public static void main(String[] args) {
        try {
            FileSystem fs = FileSystem.get(new Configuration());
            FileStatus[] statuses = fs.listStatus(new Path(args[0]));
            for (int i=0;i<statuses.length;i++) {
                Path path = statuses[i].getPath();
                String fileName = statuses[i].getPath().getName();
                String fileExtension = FilenameUtils.getExtension(fileName);
                String unixTimestamp = FilenameUtils.removeExtension(fileName);

                File tmpInputFile = File.createTempFile("hadoop", "");
                fs.copyToLocalFile(path, new Path(tmpInputFile.getPath()));

                Document doc = null;
                doc = Jsoup.parse(tmpInputFile, "UTF-8");
                Elements indexTab = doc.select("tr[href^=/cours.phtml?symbole]");

                File tmpOutput = File.createTempFile("hadoop", "__output");
                for (Element e : indexTab) {
                    writeIndiceFields(tmpOutput, e);
                }
                fs.copyFromLocalFile(new Path(tmpOutput.getPath()), new Path(args[1]+"/"+fileName));
                tmpOutput.delete();
                tmpInputFile.delete();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void writeActionFields(File outputFile, Element e) {
        System.out.println("action");
    }


    private static void writeIndiceFields(File outputFile, Element e) {
        try (PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(outputFile, true)))) { //autaumatic close
            out.println(e.text());
        } catch (IOException ex) {
            //exception handling left as an exercise for the reader
        }
    }

    private static void writeDeviceFields(File outputFile, Element e) {
        System.out.println("Device");
    }

}