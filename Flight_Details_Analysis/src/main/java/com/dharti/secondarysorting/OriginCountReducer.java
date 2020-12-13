package com.dharti.secondarysorting;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OriginCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException,
            InterruptedException {
        int bikecount = 0;
        for(IntWritable val : values)
        {
            bikecount+=val.get();
        }
        context.write(key, new IntWritable(bikecount));
    }
}

