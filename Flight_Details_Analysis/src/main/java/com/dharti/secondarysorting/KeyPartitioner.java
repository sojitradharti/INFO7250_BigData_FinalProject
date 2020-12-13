package com.dharti.secondarysorting;


import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class KeyPartitioner extends Partitioner<CompositeKey, NullWritable> {

    @Override
    public int getPartition(CompositeKey key, NullWritable value, int numPartitions){
        return key.getZip().hashCode()%numPartitions;
    }
}

