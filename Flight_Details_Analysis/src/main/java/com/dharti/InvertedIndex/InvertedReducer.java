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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class InvertedReducer extends Reducer<Text,Text,Text,Text>{
    
    private Text result = new Text();
    
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
    
    StringBuilder sb=new StringBuilder();
    boolean first = true;
    
    for(Text id:values){
    if(first){
        first = false;
    }
    else{
        sb.append("    ");
        
    }
    sb.append(id.toString());
}
    result.set(sb.toString());
    context.write(key, result);
}
}
