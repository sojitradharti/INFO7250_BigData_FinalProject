package com.dharti.citywiseflightcount;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CityCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {



	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String input = value.toString();

        String[] inputSplit = input.split(",");

        IntWritable one = new IntWritable(1);

        String city = inputSplit[8];

        context.write(new Text(city),one);
	}
}
