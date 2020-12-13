package com.dharti.citywiseflightcount;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;



public class CityCountReducer  extends Reducer<Text, IntWritable, Text, IntWritable>{
	
	protected void reduce(Text key_city, Iterable<IntWritable> values, Context context)
	
	            throws IOException,
	
	            InterruptedException
	
			{
			
			 int flight_count = 0 ;
			
			for(IntWritable val : values)
			
			        {
			
				flight_count+=val.get();
			
			        }
			
			if (key_city.getLength() > 0)
			
			context.write(key_city, new IntWritable(flight_count));
			
			}

}

