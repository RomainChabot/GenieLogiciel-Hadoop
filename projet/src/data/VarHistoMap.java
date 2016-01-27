package data;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;

public class VarHistoMap extends HashMap<Long, Double> implements Writable{
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(size());
        for(Entry<Long, Double> entry : entrySet()) {
            dataOutput.writeLong(entry.getKey());
            dataOutput.writeDouble(entry.getValue());
        }
    }
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        int mapSize = dataInput.readInt();
        for (int i = 0; i < mapSize; i++)
            put(dataInput.readLong(), dataInput.readDouble());
    }
}
