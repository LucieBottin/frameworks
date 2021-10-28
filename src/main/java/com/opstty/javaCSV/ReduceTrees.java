package com.opstty.job;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class ReduceTrees extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text t_key, Iterator<IntWritable> values, OutputCollector<Text,NullWritable> output, Reporter reporter) throws IOException {
        Text key = t_key;
        int treesByRounding = 0;
        while (values.hasNext()) {
            // replace type of value with the actual type of our value
            NullWritable value = (NullWritable) values.next();
            treesByRounding += value.get();
        }
        output.collect(key, new NullWritable(treesByRounding));
    }
}