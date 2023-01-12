package com.wenjie.mapreduce.flowphone;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    private FlowBean outV = new FlowBean();
    private Text outK = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] split = line.split("\t");
        String phone = split[1];
        outV.setPhone(Long.parseLong(phone));
        outV.setUpLine(Long.parseLong(split[split.length - 3]));
        outV.setDownLine(Long.parseLong(split[split.length - 2]));
        outV.setSumLine();
        outK.set(phone);
        context.write(outK, outV);
    }
}
