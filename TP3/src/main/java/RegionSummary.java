import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class RegionSummary implements Writable{
    private String biggestCity;
    private int biggestCityPop;
    private int regionPop;
    private int nbCities;
    private Box box;

    public RegionSummary() {
        try {
            this.box = new Box(0., 0., 0., 0.);
        } catch (IncoherentLatLongException e) {
            e.printStackTrace();
        }
    }

    public void setBiggestCity(String biggestCity) {this.biggestCity = biggestCity;}
    public void setBiggestCityPop(int biggestCityPop) {this.biggestCityPop = biggestCityPop;}
    public void setRegionPop(int regionPop) {this.regionPop = regionPop;}
    public void setNbCities(int nbCities) {this.nbCities = nbCities;}
    public void setBox(Box box) {this.box = box;}

    public RegionSummary(String biggestCity, int biggestCityPop, int regionPop, int nbCities, Box box) {
        this.biggestCity = biggestCity;
        this.biggestCityPop = biggestCityPop;
        this.regionPop = regionPop;
        this.nbCities = nbCities;
        this.box = box;
    }

    public void merge(RegionSummary regionSummary2){
        if(regionSummary2.biggestCityPop > this.biggestCityPop){
            this.biggestCity = regionSummary2.biggestCity;
            this.biggestCityPop = regionSummary2.biggestCityPop;
        }
        this.regionPop = this.regionPop + regionSummary2.regionPop;
        this.nbCities++;
        // FIXME : Ici problème, cap le nbCities a 3
        try {
            this.box.merge(regionSummary2.box);
        } catch (IncoherentLatLongException e) {
            System.out.println("Invalid region box");
            e.printStackTrace();
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(biggestCity);
        dataOutput.writeInt(biggestCityPop);
        dataOutput.writeInt(regionPop);
        dataOutput.writeInt(nbCities);
        box.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        biggestCity = dataInput.readUTF();
        biggestCityPop = dataInput.readInt();
        regionPop = dataInput.readInt();
        nbCities = dataInput.readInt();
        box.readFields(dataInput);
    }

    public int getNbCities() {
        return nbCities;
    }

    @Override
    public String toString() {
        return "RegionSummary{" +
                "biggestCity='" + biggestCity + '\'' +
                ", biggestCityPop=" + biggestCityPop +
                ", regionPop=" + regionPop +
                ", nbCities=" + nbCities +
                ", box=" + box +
                '}';
    }
}
