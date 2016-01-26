package data;

import org.apache.hadoop.io.Writable;
import org.jsoup.nodes.Element;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Action implements Writable {
    private long timestamp;
    private String libelle;
    private Double last;
    private Double var;
    private Double open;
    private Double high;
    private Double low;
    private Double varAn;
    private Double totVolume;

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(timestamp);
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
        timestamp = in.readLong();
        libelle = in.readUTF();
        last = in.readDouble();
        var = in.readDouble();
        open = in.readDouble();
        high = in.readDouble();
        low = in.readDouble();
        varAn = in.readDouble();
        totVolume = in.readDouble();
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getLibelle() {
        return libelle;
    }

    public void setLibelle(String libelle) {
        this.libelle = libelle;
    }

    public Double getLast() {
        return last;
    }

    public void setLast(Double last) {
        this.last = last;
    }

    public Double getVar() {
        return var;
    }

    public void setVar(Double var) {
        this.var = var;
    }

    public Double getOpen() {
        return open;
    }

    public void setOpen(Double open) {
        this.open = open;
    }

    public Double getHigh() {
        return high;
    }

    public void setHigh(Double high) {
        this.high = high;
    }

    public Double getLow() {
        return low;
    }

    public void setLow(Double low) {
        this.low = low;
    }

    public Double getVarAn() {
        return varAn;
    }

    public void setVarAn(Double varAn) {
        this.varAn = varAn;
    }

    public Double getTotVolume() {
        return totVolume;
    }

    public void setTotVolume(Double totVolume) {
        this.totVolume = totVolume;
    }

    @Override
    public String toString() {
        return "Action{" +
                "timestamp=" + timestamp +
                ", libelle='" + libelle + '\'' +
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
        if (tokens.length == 9){
            action.setLibelle(tokens[1]);
            action.setTimestamp(Long.valueOf(tokens[0]));
            action.setLast(getValue(tokens[2]));
            action.setVar(getValue(tokens[3]));
            action.setOpen(getValue(tokens[4]));
            action.setHigh(getValue(tokens[5]));
            action.setLow(getValue(tokens[6]));
            action.setVarAn(getValue(tokens[7]));
            action.setTotVolume(getValue(tokens[8]));
            return action;
        } else {
            return null;
        }
    }
    
    private static Double getValue(String valueToParse){
        if (valueToParse.equals("ND")) { return null; }
        Double res = null;
        try { res = Double.valueOf(valueToParse);} catch (NumberFormatException e){}
        return res;
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
