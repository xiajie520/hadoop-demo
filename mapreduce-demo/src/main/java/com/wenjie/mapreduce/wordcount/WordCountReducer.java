package com.wenjie.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * * KEYIN, 数据key Text
 * * VALUEIN, 单词数量 IntWritable
 * * KEYOUT, 输出数据key Text
 * * VALUEOUT 输出单词条数 IntWritable
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable out = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
       int sum = 0;

        for (IntWritable value : values) {
            sum+=value.get();
        }

        out.set(sum);
        context.write(key,out);
    }
}
