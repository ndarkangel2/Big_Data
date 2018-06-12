package edu.umkc.cjk8zb;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Mapper2 extends
        Mapper<Object, Text, Text, IntWritable> {

    private Text word = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] csv = value.toString().split("\t");
        String name = csv[0];
        int intValue = Integer.parseInt(csv[1]);
        word.set(name);
        context.write(word, new IntWritable(intValue));
    }
}
