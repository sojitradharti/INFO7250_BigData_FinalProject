package com.dharti.secondarysorting;


import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SecSortReducer extends Reducer<CompositeKey, NullWritable, CompositeKey, NullWritable> {

    protected void reduce(CompositeKey key, Iterable<NullWritable> values, Context context)
            throws IOException,
            InterruptedException
    {
       for(NullWritable v: values)
       {
           context.write(key,v);
       }
    }
}

