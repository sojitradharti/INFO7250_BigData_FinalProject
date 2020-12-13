package com.dharti.secondarysorting;


import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class SecSortDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance();

        job.setJarByClass(SecSortDriver.class);

        job.setGroupingComparatorClass(SecSortGroupComparator.class);
        job.setSortComparatorClass(SecSortComparator.class);
        job.setPartitionerClass(KeyPartitioner.class);

        TextInputFormat.addInputPath(job, new Path(args[0]));
        Path outdir = new Path(args[1]);
        TextOutputFormat.setOutputPath(job, outdir);

        job.setMapperClass(SecSortMapper.class);
        job.setReducerClass(SecSortReducer.class);

        job.setNumReduceTasks(1);

        job.setOutputKeyClass(CompositeKey.class);
        job.setOutputValueClass(NullWritable.class);

        FileSystem fs = FileSystem.get(job.getConfiguration());
        if(fs.exists(outdir))
        {
            fs.delete(outdir, true);
        }
        boolean result = job.waitForCompletion(true);

        if(result)
        {
            Job job1 = Job.getInstance();
            job1.setJarByClass(SecSortDriver.class);

            job1.setMapperClass(OriginCountMapper.class);
            job1.setReducerClass(OriginCountReducer.class);

            job1.setOutputKeyClass(Text.class);
            job1.setOutputValueClass(IntWritable.class);

            TextInputFormat.addInputPath(job1, new Path(args[1]));
            Path finalOutputDirectory = new Path(args[2]);
            TextOutputFormat.setOutputPath(job1, finalOutputDirectory);

            FileSystem fs2 = FileSystem.get(job1.getConfiguration());
            if(fs2.exists(finalOutputDirectory))
            {
                fs2.delete(finalOutputDirectory, true);
            }

            job1.waitForCompletion(true);

        }

    }
}

