package com.dharti.citywiseflightcount;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


 


 

public class CityCountDriver {

public static void main(String[] args) throws IOException, ClassNotFoundException,

InterruptedException {

        Configuration conf = new Configuration();

        FileSystem fs =FileSystem.get(conf);

        if(fs.exists(new Path(args[1])))

        {

            fs.delete(new Path(args[1]), true);

        }

        Job job = Job.getInstance(conf);

       

        job.setMapperClass(CityCountMapper.class);

        job.setReducerClass(CityCountReducer.class);

       

        // to provide more optimized input to the Reducer class

       

        job.setJarByClass(CityCountDriver.class);

        job.setNumReduceTasks(1);

        TextInputFormat.addInputPath(job, new Path(args[0]));

        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapOutputKeyClass(Text.class);

        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);

        job.setOutputValueClass(IntWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);

        

    }

}