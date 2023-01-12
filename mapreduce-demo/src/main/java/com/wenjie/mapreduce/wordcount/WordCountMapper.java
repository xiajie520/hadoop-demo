package com.wenjie.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * KEYIN, 输入数据偏移量 LongWritable
 * VALUEIN, 输入数据Text
 * KEYOUT, 输出数据key Text
 * VALUEOUT 输出单词条数 IntWritable
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Text text = new Text();
    private IntWritable writable = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split(" ");
        for (String word : words) {
            text.set(word);
            context.write(text, writable);
        }
    }
}
