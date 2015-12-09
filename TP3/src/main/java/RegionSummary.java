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

    public String getBiggestCity() {return biggestCity;}
    public int getBiggestCityPop() {return biggestCityPop;}
    public int getRegionPop() {return regionPop;}
    public int getNbCities() {return nbCities;}
    public Box getBox() {return box;}

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
        if(regionSummary2.getBiggestCityPop() > getBiggestCityPop()){
            setBiggestCity(regionSummary2.getBiggestCity());
            setBiggestCityPop(regionSummary2.getBiggestCityPop());
        }
        //TODO: a finir
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeChars(biggestCity);
        dataOutput.writeInt(biggestCityPop);
        dataOutput.writeInt(regionPop);
        dataOutput.writeInt(nbCities);
        box.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        biggestCity = dataInput.readLine();
        biggestCityPop = dataInput.readInt();
        regionPop = dataInput.readInt();
        nbCities = dataInput.readInt();
        box.readFields(dataInput);
    }
}
