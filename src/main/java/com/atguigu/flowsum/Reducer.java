package com.atguigu.flowsum;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class Reducer extends org.apache.hadoop.mapreduce.Reducer <LongWritable, FlowBean, LongWritable, FlowBean> {

    FlowBean v = new FlowBean();
    @Override
    protected void reduce(LongWritable key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        long upFlow = 0;
        long downFlow = 0;
        long sumFlow =0;

        for (FlowBean value : values) {
            upFlow = value.getUpFlow();
            downFlow = value.getDownFlow();
        }
        v.set(upFlow,downFlow);

        context.write(key, v);


    }
}
