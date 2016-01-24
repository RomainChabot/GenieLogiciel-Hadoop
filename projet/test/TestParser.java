import data.Indice;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.*;

/**
 * Created by rchabot on 22/01/16.
 */
public class TestParser {
    public static void listFilesForFolder(final File folder) {

    }

    public static void main(String[] args) {
        final File inputFolder = new File(args[0]);
        final File outputFolder = new File(args[1]);
        Document doc = null;
        for (final File input : inputFolder.listFiles()) {
            try {
                System.out.println(input.getName());
                doc = Jsoup.parse(input, "UTF-8");
                Elements indexTab = doc.select("tr[href^=/cours.phtml?symbole]");
                for (Element e : indexTab) {
                    Indice ind = new Indice();
                    ind.setLibelle(e.getElementsByClass("tdv-libelle").text());
                    try {
                        ind.setLast(Double.valueOf(e.getElementsByClass("tdv-last").text().replaceAll(" ", "")));
                    }catch (NumberFormatException ex){
                        ind.setLast(0.0);
                    }
                    try {
                        ind.setVar(Double.valueOf(e.getElementsByClass("tdv-var").text().split("%")[0]));
                    }catch (NumberFormatException ex){
                        ind.setVar(0.0);
                    }
                    try {
                        ind.setOpen(Double.valueOf(e.getElementsByClass("tdv-open").text().replaceAll(" ", "").split("%")[0]));
                    } catch (NumberFormatException ex){
                        ind.setOpen(0.0);
                    }
                    try {
                        ind.setHigh(Double.valueOf(e.getElementsByClass("tdv-high").text().replaceAll(" ", "").split("%")[0]));
                    } catch (NumberFormatException ex){
                        ind.setHigh(0.0);
                    }
                    try {
                        ind.setLow(Double.valueOf(e.getElementsByClass("tdv-low").text().replaceAll(" ", "").split("%")[0]));
                    } catch (NumberFormatException ex){
                        ind.setLow(0.0);
                    }
                    try {
                        ind.setPrevClose(Double.valueOf(e.getElementsByClass("tdv-prev_close").text().replaceAll(" ", "").split("%")[0]));
                    } catch (NumberFormatException ex){
                        ind.setPrevClose(0.0);
                    }
                    try {
                        ind.setVarAn(Double.valueOf(e.getElementsByClass("tdv-var_an").text().replaceAll(" ", "").split("%")[0]));
                    } catch (NumberFormatException ex){
                        ind.setVarAn(0.0);
                    }

                    System.out.println(ind.toString());
                    File action = new File(outputFolder, ind.getLibelle());
                    if (!action.exists()) {
                        action.createNewFile();
                    }

                    try(PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(action, true)))) {
                        out.println(ind.toString());
                        out.close();
                    }catch (IOException ex) {
                        //exception handling left as an exercise for the reader
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }


}
