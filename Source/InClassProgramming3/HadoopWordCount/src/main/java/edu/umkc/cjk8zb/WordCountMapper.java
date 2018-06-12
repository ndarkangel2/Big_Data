package edu.umkc.cjk8zb;

/**
 * Created by Mayanka on 03-Sep-15.
 */
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends
        Mapper<Object, Text, Text, IntWritable> {

    private Text word = new Text();
    private int rows = -1;
    private int cols = -1;

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] csv = value.toString().split(" ");
        if (this.rows < 0) {
            this.rows = Integer.parseInt(csv[0]);
            this.cols = Integer.parseInt(csv[1]);
            return;
        }

        String matrix = csv[0];
        String rowPos = csv[1];
        String colPos = csv[2];

        int intValue = Integer.parseInt(csv[3]);
        System.out.println(intValue);
        if (matrix.equals("A")) {
            for (int c = 1; c <= cols; c++) {
                String newKey = "A " + rowPos + " " + colPos + " B " + colPos + " " + String.valueOf(c);
                word.set(newKey);
                System.out.println(newKey);
                context.write(word, new IntWritable(intValue));
            }
        } else {
            for (int r = 1; r <= rows; r++) {
                String newKey = "A " + String.valueOf(r) + " " + rowPos + " B " + rowPos + " " + colPos;
                word.set(newKey);
                context.write(word, new IntWritable(intValue));
            }
        }
    }
}
