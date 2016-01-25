package data;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

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

    public static Action getFromCSV(Text value) {
        Action action = new Action();
        String tokens[] = value.toString().split(";");
        action.setLibelle(tokens[0]);
        try { action.setLast(Double.valueOf(tokens[1]));} catch (NumberFormatException e){}
        try { action.setVar(Double.valueOf(tokens[2]));} catch (NumberFormatException e){}
        try { action.setOpen(Double.valueOf(tokens[3]));} catch (NumberFormatException e){}
        try { action.setHigh(Double.valueOf(tokens[4]));} catch (NumberFormatException e){}
        try { action.setLow(Double.valueOf(tokens[5]));} catch (NumberFormatException e){}
        try { action.setVarAn(Double.valueOf(tokens[6]));} catch (NumberFormatException e){}
        try { action.setTotVolume(Double.valueOf(tokens[7]));} catch (NumberFormatException e){}
        return action;
    }
}
