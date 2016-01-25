package data;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TopKVal extends Pair<BoundaryDate, Double> implements Writable{
    public BoundaryDate getBoundaryDate() {
        return fst;
    }
    public void setBoundaryDate(BoundaryDate bDate) {
        this.fst = bDate;
    }
    public Double getVal() {
        return snd;
    }
    public void setVal(Double val) {
        this.snd = val;
    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(fst.toString());
        dataOutput.writeDouble(snd);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        fst = BoundaryDate.strToEnum(dataInput.readUTF());
        snd = dataInput.readDouble();
    }
}
