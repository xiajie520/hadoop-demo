package com.wenjie.mapreduce.flowphone;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
    private FlowBean outV = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        long up = 0;
        long down = 0;
        long sum = 0;
        for (FlowBean value : values) {
            up += value.getUpLine();
            down += value.getDownLine();
            sum += value.getSumLine();
        }
        outV.setUpLine(up);
        outV.setDownLine(down);
        outV.setSumLine();
        context.write(key, outV);
    }
}
