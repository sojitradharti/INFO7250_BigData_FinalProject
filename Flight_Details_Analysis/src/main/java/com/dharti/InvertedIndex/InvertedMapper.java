package com.dharti.InvertedIndex;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


public class InvertedMapper extends Mapper<LongWritable, Text, Text, Text>{
    
    private Text city = new Text();
    private Text year = new Text();
    
    public void map(LongWritable key, Text values, Context context){
        
        if(key.get()==0){
            return;
        }
        
        try{
        String[] tokens = values.toString().split(",");
        
        year.set(tokens[5]);
        city.set(tokens[1]);
        
        context.write(city, year);
        }
        catch(Exception ex){
            System.out.println("Error in Mapper" + ex.getMessage());
        }
    }
}