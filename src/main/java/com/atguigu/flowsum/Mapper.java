package com.atguigu.flowsum;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class Mapper extends org.apache.hadoop.mapreduce.Mapper < LongWritable, Text, LongWritable, FlowBean> {

    LongWritable k = new LongWritable();
    FlowBean v = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {


        String line = value.toString();
        String [] fields= line.split("\t");

        k.set(Long.parseLong(fields[1]));
        long upstream = Long.parseLong(fields[fields.length-3]);
        long downstream = Long.parseLong(fields[fields.length-2]);
        v.set(upstream,downstream);

        context.write(k, v);

    }
}
