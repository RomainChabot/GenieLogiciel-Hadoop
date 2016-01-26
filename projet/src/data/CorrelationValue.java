package data;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CorrelationValue extends Pair<Double, WritableActionMap> implements Writable{

    public Double getMean() {
        return fst;
    }
    public void setMean(Double variance) {
        fst = variance;
    }
    public Double getVariation(Long timestamp){
        return snd.get(timestamp);
    }
    public void setVariation(Long timestamp, Double variation){
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
