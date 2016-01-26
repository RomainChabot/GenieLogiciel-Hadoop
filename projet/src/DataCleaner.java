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

public class DataCleaner {
    private String srcFolder;
    private String dstFolder;

    public DataCleaner(String srcFolder, String dstFolder){
        this.srcFolder = srcFolder;
        this.dstFolder = dstFolder;
    }

    public void run() {
        System.out.println("Cleaning in progress... ");
        try {
            FileSystem fs = FileSystem.get(new Configuration());
            FileStatus[] statuses = fs.listStatus(new Path(srcFolder));
            for (int i=0;i<statuses.length;i++) {
                Path path = statuses[i].getPath();
                String fileName = statuses[i].getPath().getName();
                String fileExtension = FilenameUtils.getExtension(fileName);
                String unixTimestamp = FilenameUtils.removeExtension(fileName);

                File tmpInputFile = File.createTempFile("hadoop", "");
                fs.copyToLocalFile(path, new Path(tmpInputFile.getPath()));
                Document doc = null;
                doc = Jsoup.parse(tmpInputFile, "UTF-8");

                File tmpOutput = File.createTempFile("hadoop", "__output");

                if (fileExtension.equals("dev")){

                } else if (fileExtension.equals("srd")){
                    Elements indexTab = doc.select("div.main-content > table > tbody > tr");
                    for (Element e : indexTab) {
                        writeActionFields(tmpOutput, e);
                    }
                } else if (fileExtension.equals("ind")){
                    Elements indexTab = doc.select("tr[href^=/cours.phtml?symbole]");
                    for (Element e : indexTab)
                    {
                        writeIndiceFields(tmpOutput, e);
                    }
                } else {
                    System.out.println("Extension de fichier inconnue : "+fileExtension);
                }

                fs.copyFromLocalFile(new Path(tmpOutput.getPath()), new Path(dstFolder+"/"+fileName));
                tmpOutput.delete();
                tmpInputFile.delete();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("End of data cleaning !");
    }

    private static void writeActionFields(File outputFile, Element e) {
        try (PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(outputFile, true)))) { //autaumatic close
            out.print(e.getElementsByClass("tdv-libelle").text()+";");
            out.print(e.getElementsByClass("tdv-last").text().replaceAll(" ", "").replaceAll("\\(c\\)", "").replaceAll("\\(s\\)", "")+";");
            out.print(e.getElementsByClass("tdv-var").text().replaceAll("%", "")+";");
            out.print(e.getElementsByClass("tdv-open").text().replaceAll(" ", "")+";");
            out.print(e.getElementsByClass("tdv-high").text().replaceAll(" ", "")+";");
            out.print(e.getElementsByClass("tdv-low").text().replaceAll(" ", "")+";");
            out.print(e.getElementsByClass("tdv-var_an").text().replaceAll(" ", "").replaceAll("%", "")+";");
            out.println(e.getElementsByClass("tdv-tot_volume").text().replaceAll(" ", ""));
        } catch (IOException ex) {
            //exception handling left as an exercise for the reader
        }
    }


    private static void writeIndiceFields(File outputFile, Element e) {
        try (PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(outputFile, true)))) { //autaumatic close
            out.print(e.getElementsByClass("tdv-libelle").text()+";");
            out.print(e.getElementsByClass("tdv-last").text()+";");
            out.print(e.getElementsByClass("tdv-var").text()+";");
            out.print(e.getElementsByClass("tdv-open").text()+";");
            out.print(e.getElementsByClass("tdv-high").text()+";");
            out.print(e.getElementsByClass("tdv-low").text()+";");
            out.print(e.getElementsByClass("tdv-prev_close").text()+";");
            out.println(e.getElementsByClass("tdv-var_an").text());
        } catch (IOException ex) {
            //exception handling left as an exercise for the reader
        }
    }

    private static void writeDeviceFields(File outputFile, Element e) {
        System.out.println("Device");
    }

}
