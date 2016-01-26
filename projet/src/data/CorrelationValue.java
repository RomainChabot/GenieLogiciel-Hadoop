package data;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CorrelationValue extends Pair<Double, WritableActionMap> implements Writable{

    public CorrelationValue(){
        fst = new Double(0.0);
        snd = new WritableActionMap();
    }


    public Double getAvg() {
        return fst;
    }
    public void setAvg(Double variance) {
        fst = variance;
    }
    public WritableActionMap getVariation(){
        return snd;
    }
    public Double getVariationAt(Long timestamp){
        return snd.get(timestamp);
    }
    public void setVariation(WritableActionMap actionMap){
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
