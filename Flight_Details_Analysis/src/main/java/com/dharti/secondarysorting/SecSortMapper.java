package com.dharti.secondarysorting;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SecSortMapper extends Mapper<LongWritable, Text, CompositeKey, NullWritable> {

    protected void map(LongWritable key,
                       Text value,Context context)
            throws IOException,
            InterruptedException{
        String line  = value.toString();
        String[] tokens = line.split(",");
        String zip = null;
        String bikeId = null;

        try {
        	zip = tokens[9];
            bikeId = tokens[8];
            //zip = tokens[10];
            //bikeId = tokens[8];
        }

        catch (Exception e)
        {

        }

        if(zip!=null && bikeId!=null)
        {
            CompositeKey outKey = new CompositeKey(zip, bikeId);
            context.write(outKey, NullWritable.get());
        }

    }

}
