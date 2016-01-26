package data;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CorrelationKey extends Pair<String, String> implements WritableComparable{
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CorrelationKey that = (CorrelationKey) o;

        if (fst != null ? !fst.equals(that.fst) : that.fst != null) return false;
        return !(snd != null ? !snd.equals(that.snd) : that.snd != null);

    }

    @Override
    public int hashCode() {
        int result = fst != null ? fst.hashCode() : 0;
        result = 31 * result + (snd != null ? snd.hashCode() : 0);
        return result;
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
    public int compareTo(Object o) {
        CorrelationKey key = (CorrelationKey) o;
        int comp1 = fst.compareTo(((CorrelationKey) o).getAction1());
        if(comp1 != 0)
            return comp1;
        else
            return snd.compareTo(((CorrelationKey) o).getAction2());
    }

    @Override
    public String toString() {
        return "CorrelationKey{" +
                "fst='" + fst + '\'' +
                ", snd='" + snd + '\'' +
                '}';
    }
}
