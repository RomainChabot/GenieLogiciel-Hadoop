package data;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Indice implements Writable {
    private String libelle;
    private double last;
    private double var;
    private double open;
    private double high;
    private double low;
    private double prevClose;
    private double varAn;

    @Override
    public String toString() {
        return "Indice{" +
                "libelle='" + libelle + '\'' +
                ", last=" + last +
                ", var=" + var +
                ", open=" + open +
                ", high=" + high +
                ", low=" + low +
                ", prevClose=" + prevClose +
                ", varAn=" + varAn +
                '}';
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(libelle);
        out.writeDouble(last);
        out.writeDouble(var);
        out.writeDouble(open);
        out.writeDouble(high);
        out.writeDouble(low);
        out.writeDouble(prevClose);
        out.writeDouble(varAn);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        libelle = in.readUTF();
        last = in.readDouble();
        var = in.readDouble();
        open = in.readDouble();
        high = in.readDouble();
        low = in.readDouble();
        prevClose = in.readDouble();
        varAn = in.readDouble();
    }

    public String getLibelle() {
        return libelle;
    }

    public void setLibelle(String libelle) {
        this.libelle = libelle;
    }

    public double getLast() {
        return last;
    }

    public void setLast(double last) {
        this.last = last;
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

    public double getPrevClose() {
        return prevClose;
    }

    public void setPrevClose(double prevClose) {
        this.prevClose = prevClose;
    }

    public double getVarAn() {
        return varAn;
    }

    public void setVarAn(double varAn) {
        this.varAn = varAn;
    }
}
