package com.wenjie.mapreduce.flowphone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        // 1、设置主类
        job.setJarByClass(FlowDriver.class);
        // 2、设置mapper和reduce类
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);
        // 3、设置mapper输出格式
        job.setMapOutputValueClass(FlowBean.class);
        job.setMapOutputKeyClass(Text.class);
        // 4、设置最终输出格式
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        // 5、设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path("E:\\11_input\\inputflow"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\11_output\\outputflow2"));
        // 6、任务提交
        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 : 1);
    }
}
