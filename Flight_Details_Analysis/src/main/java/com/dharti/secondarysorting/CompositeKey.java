package com.dharti.secondarysorting;


import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CompositeKey implements WritableComparable<CompositeKey> {

    private String zip;
    private String bikeId;

    public CompositeKey(){
        super();
    }

    public CompositeKey(String zip, String bikeId)
    {
        super();
        this.zip=zip;
        this.bikeId=bikeId;
    }

    public int compareTo(CompositeKey o) {
        int result = this.zip.compareTo(o.zip);
        if(result==0){
            return this.bikeId.compareTo(o.bikeId);
        }
        return result;
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(zip);
        out.writeUTF(bikeId);
    }

    public void readFields(DataInput in) throws IOException {
        zip=in.readUTF();
        bikeId=in.readUTF();
    }

    public String getZip() {
        return zip;
    }

    public void setZip(String zip) {
        this.zip = zip;
    }

    public String getBikeId() {
        return bikeId;
    }

    public void setBikeId(String bikeId) {
        this.bikeId = bikeId;
    }

    @Override
    public String toString()
    {
        return zip + "," +bikeId;
    }

}

