package data;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CorrelationValue extends Pair<Double, WritableActionMap> implements Writable{

    public Double getVariance() {
        return fst;
    }
    public void setVariance(Double variance) {
        fst = variance;
    }
//    public Map<Long, Double> getVariations() {
//        return snd;
//    }
//    public void setVariations(WritableActionMap variations) {
//        snd = variations;
//    }
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
