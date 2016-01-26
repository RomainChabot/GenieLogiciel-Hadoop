package data;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by rchabot on 26/01/16.
 */
public class StringPair extends Pair<String, String> implements Writable {
    public StringPair(){}

    public StringPair(String action1, String action2){
        setActions(action1, action2);
    }

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

    @Override
    public String toString() {
        return "("+fst+","+snd+")";
    }
}
