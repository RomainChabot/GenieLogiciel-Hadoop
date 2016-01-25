package data;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CorrelationKey extends Pair<String, String> implements Writable{
    public String getAction1() {
        return fst;
    }
    public String getAction2() {
        return snd;
    }
    public void setActions(String action1, String action2) {
        if(action1.compareTo(action2) < 0){
            this.fst = action1;
            this.snd = action2;
        }
        else {
            this.fst = action2;
            this.snd = action1;
        }
    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(fst);
        dataOutput.writeUTF(snd);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        fst = dataInput.readUTF();
        snd = dataInput.readUTF();
    }


}
