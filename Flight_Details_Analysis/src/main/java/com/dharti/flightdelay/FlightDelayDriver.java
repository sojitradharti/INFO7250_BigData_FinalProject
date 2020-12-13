package com.dharti.flightdelay;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class FlightDelayDriver  {
	public static void main(String[] args) throws Exception {

		 Configuration conf = new Configuration();

	        FileSystem fs =FileSystem.get(conf);

	        if(fs.exists(new Path(args[1])))

	        {

	            fs.delete(new Path(args[1]), true);

	        }

	        Job job1 = Job.getInstance(conf);
		job1.setJarByClass(FlightDelayDriver.class);
		job1.setJobName("Daily delay ratio with delay > 15 minutes and cancelled flights ratio");

		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(FlightDelayTuple.class);
		
		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);
		
		job1.setMapperClass(FlightDelayMapper.class);
		job1.setCombinerClass(FlightDelayReducer.class);
		job1.setReducerClass(FlightDelayReducer.class);
		
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(FlightDelayTuple.class);
		
	

		FileInputFormat.setInputPaths(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));
		  //TextInputFormat.addInputPath(job1, new Path(args[0]));

	      //TextOutputFormat.setOutputPath(job1, new Path(args[1]));
		

		

		

		if (!job1.waitForCompletion(true)) {
			System.exit(1);
		}

	}

}
