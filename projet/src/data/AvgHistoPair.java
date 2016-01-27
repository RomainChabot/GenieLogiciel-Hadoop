package data;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AvgHistoPair extends Pair<Double, VarHistoMap> implements Writable{

    public AvgHistoPair(){
        fst = new Double(0.0);
        snd = new VarHistoMap();
    }


    public Double getAvg() {
        return fst;
    }
    public void setAvg(Double variance) {
        fst = variance;
    }
    public VarHistoMap getVariation(){
        return snd;
    }
    public Double getVariationAt(Long timestamp){
        return snd.get(timestamp);
    }
    public void setVariation(VarHistoMap actionMap){
        snd = actionMap;
    }
    public void putVariation(Long timestamp, Double variation){
        snd.put(timestamp, variation);
    }
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(fst);
        snd.write(dataOutput);
    }
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        fst = dataInput.readDouble();
        snd.readFields(dataInput);
    }
}
