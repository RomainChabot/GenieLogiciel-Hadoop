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

        Elements indexTab = doc.select("tr[href^=/cours.phtml?symbole]");
        for (Element e : indexTab)
        {
            System.out.println(e.getElementsByClass("tdv-libelle").text());
        }
    }
}
