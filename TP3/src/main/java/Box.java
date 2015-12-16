import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Box implements Writable{
    private double minLat;
    private double maxLat;
    private double minLong;
    private double maxLong;

    public Box(double minLat, double maxLat, double minLong, double maxLong) throws IncoherentLatLongException {
        this.minLat = minLat;
        this.maxLat = maxLat;
        this.minLong = minLong;
        this.maxLong = maxLong;
        checkLatLongs();
    }

    public double area(){
        return (maxLat - minLat) * (maxLong - minLong);
    }

    private void checkLatLongs() throws IncoherentLatLongException{
        if(minLat > maxLat)
            throw new IncoherentLatLongException("The latitude " + minLat +
                    "is greater than the latitude" + maxLat + ", but should be lower");
        if(minLong > minLong)
            throw new IncoherentLatLongException("The longitude " + minLong +
                    "is greater than the longitude" + maxLong + ", but should be lower");
    }

   public void merge(Box box2) throws IncoherentLatLongException {
       box2.checkLatLongs();
       this.minLat = Math.min(this.minLat, box2.minLat);
       this.maxLat = Math.max(this.maxLat, box2.maxLat);
       this.minLong = Math.min(this.minLong, box2.minLong);
       this.maxLong = Math.max(this.maxLong, box2.maxLong);
   }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(minLat);
        dataOutput.writeDouble(maxLat);
        dataOutput.writeDouble(minLong);
        dataOutput.writeDouble(maxLong);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        minLat = dataInput.readDouble();
        maxLat = dataInput.readDouble();
        minLong = dataInput.readDouble();
        maxLong = dataInput.readDouble();

    }

    @Override
    public String toString() {
        return "Box{" +
                "minLat=" + minLat +
                ", maxLat=" + maxLat +
                ", minLong=" + minLong +
                ", maxLong=" + maxLong +
                '}';
    }
}
