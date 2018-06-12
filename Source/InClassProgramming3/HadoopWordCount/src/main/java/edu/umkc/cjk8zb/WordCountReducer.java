package edu.umkc.cjk8zb;

/**
 * Created by Mayanka on 03-Sep-15.
 */
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends
        Reducer<Text, IntWritable, Text, IntWritable> {

    private Text word = new Text();

    public void reduce(Text text, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int multi = 1;
        for (IntWritable value : values) {
            multi *= value.get();
        }
        String[] parts = text.toString().split(" ");
        // A 1 1 B 2 2
        String newKey = "R " + parts[1]  + " " + parts[5];
        word.set(newKey);
        context.write(word, new IntWritable(multi));
    }
}