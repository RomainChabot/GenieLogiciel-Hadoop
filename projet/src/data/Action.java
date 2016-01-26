package data;

import org.apache.hadoop.io.Writable;
import org.jsoup.nodes.Element;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by rchabot on 23/01/16.
 */
public class Action implements Writable {
    private String libelle;
    private double last;
    private double var;
    private double open;
    private double high;
    private double low;
    private double varAn;
    private double totVolume;

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(libelle);
        out.writeDouble(last);
        out.writeDouble(var);
        out.writeDouble(open);
        out.writeDouble(high);
        out.writeDouble(low);
        out.writeDouble(varAn);
        out.writeDouble(totVolume);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        libelle = in.readUTF();
        last = in.readDouble();
        var = in.readDouble();
        open = in.readDouble();
        high = in.readDouble();
        low = in.readDouble();
        varAn = in.readDouble();
        totVolume = in.readDouble();
    }

    public double getLast() {
        return last;
    }

    public void setLast(double last) {
        this.last = last;
    }

    public String getLibelle() {
        return libelle;
    }

    public void setLibelle(String libelle) {
        this.libelle = libelle;
    }

    public double getVar() {
        return var;
    }

    public void setVar(double var) {
        this.var = var;
    }

    public double getOpen() {
        return open;
    }

    public void setOpen(double open) {
        this.open = open;
    }

    public double getHigh() {
        return high;
    }

    public void setHigh(double high) {
        this.high = high;
    }

    public double getLow() {
        return low;
    }

    public void setLow(double low) {
        this.low = low;
    }

    public double getVarAn() {
        return varAn;
    }

    public void setVarAn(double varAn) {
        this.varAn = varAn;
    }

    public double getTotVolume() {
        return totVolume;
    }

    public void setTotVolume(double totVolume) {
        this.totVolume = totVolume;
    }

    @Override
    public String toString() {
        return "Action{" +
                "libelle='" + libelle + '\'' +
                ", var=" + var +
                ", open=" + open +
                ", high=" + high +
                ", low=" + low +
                ", varAn=" + varAn +
                ", totVolume=" + totVolume +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Action action = (Action) o;

        return !(libelle != null ? !libelle.equals(action.libelle) : action.libelle != null);

    }

    public static Action getFromCSV(String actionCSV) {
        Action action = new Action();
        String tokens[] = actionCSV.split(";");
        if (tokens.length == 8){
            action.setLibelle(tokens[0]);
            try {
                action.setLast(Double.valueOf(tokens[1]));
                action.setVar(Double.valueOf(tokens[2]));
                action.setOpen(Double.valueOf(tokens[3]));
                action.setHigh(Double.valueOf(tokens[4]));
                action.setLow(Double.valueOf(tokens[5]));
                action.setVarAn(Double.valueOf(tokens[6]));
                action.setTotVolume(Double.valueOf(tokens[7]));
                return action;
            }catch (NumberFormatException e){
                return null;
            }
        } else {
            return null;
        }
    }

    public static String convertToCSV(Element e, long unixTimestamp) {
        StringBuilder builder = new StringBuilder();
        builder.append(unixTimestamp+";");
        builder.append(e.getElementsByClass("tdv-libelle").text()+";");
        builder.append(e.getElementsByClass("tdv-last").text().replaceAll(" ", "").replaceAll("\\(c\\)", "").replaceAll("\\(s\\)", "")).append(";");
        builder.append(e.getElementsByClass("tdv-var").text().replaceAll("%", "")+";");
        builder.append(e.getElementsByClass("tdv-open").text().replaceAll(" ", "")+";");
        builder.append(e.getElementsByClass("tdv-high").text().replaceAll(" ", "")+";");
        builder.append(e.getElementsByClass("tdv-low").text().replaceAll(" ", "")+";");
        builder.append(e.getElementsByClass("tdv-var_an").text().replaceAll(" ", "").replaceAll("%", "")+";");
        builder.append(e.getElementsByClass("tdv-tot_volume").text().replaceAll(" ", ""));
        return builder.toString();
    }
}
