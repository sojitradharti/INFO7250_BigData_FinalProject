package com.dharti.average_flight;

import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



import java.net.URI;


public class AverageDriver {
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();

		FileSystem hdfs = FileSystem.get(URI.create("hdfs://localhost:9000"), conf);

		Path output = new Path(args[1]);
		// delete existing directory
		if (hdfs.exists(output)) {
			hdfs.delete(output, true);
		}

		// Create a new Job
		Job job1 = Job.getInstance(conf, "Average Distance and Airtime of Carrier");
		job1.setJarByClass(AverageDriver.class);

		// Specify various job-specific parameters
		job1.setJobName("Average Distance and Airtime Carrier");

		

		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);

		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(AvgDisTimeTuple.class);

		job1.setMapperClass(AverageMapper.class);
		job1.setCombinerClass(AverageCombiner.class);
		job1.setReducerClass(AverageReducer.class);

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(AvgDisTimeTuple.class);


        TextInputFormat.addInputPath(job1, new Path(args[0]));
        Path outDir = new Path(args[1]);
        TextOutputFormat.setOutputPath(job1, outDir);

        FileSystem fs = FileSystem.get(job1.getConfiguration());
        if(fs.exists(outDir)){
            fs.delete(outDir, true);
        }
        job1.waitForCompletion(true);
	}

}
