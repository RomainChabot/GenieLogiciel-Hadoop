import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.File;
import java.io.IOException;

/**
 * Created by rchabot on 22/01/16.
 */

public class TestParser {
    public static void main(String[] args) {
        Document doc = null;
        try {
            File input = new File(args[0]);
            doc = Jsoup.parse(input, "UTF-8");
        } catch (IOException e) {
            e.printStackTrace();
        }

        Elements tab = doc.select("div.main-content > table > tbody > tr");
        int cpt = 0;
        for (Element e : tab){
            System.out.print(e.getElementsByClass("tdv-libelle").text()+";");
            System.out.print(e.getElementsByClass("tdv-last").text().replaceAll(" ", "").replaceAll("\\(c\\)", "").replaceAll("\\(s\\)", "")+";");
            System.out.print(e.getElementsByClass("tdv-var").text().replaceAll("%", "")+";");
            System.out.print(e.getElementsByClass("tdv-open").text().replaceAll(" ", "")+";");
            System.out.print(e.getElementsByClass("tdv-high").text().replaceAll(" ", "")+";");
            System.out.print(e.getElementsByClass("tdv-low").text().replaceAll(" ", "")+";");
            System.out.print(e.getElementsByClass("tdv-var_an").text().replaceAll(" ", "").replaceAll("%", "")+";");
            System.out.println(e.getElementsByClass("tdv-tot_volume").text().replaceAll(" ", "")+";");
            cpt++;
        }
        System.out.println(cpt);

        /*Elements indexTab = doc.select("tr[href^=/cours.phtml?symbole]");
        for (Element e : indexTab)
        {
            System.out.println(e.getElementsByClass("tdv-libelle").text());
        }*/
    }
}
