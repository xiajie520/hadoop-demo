package com.wenjie.hdfs.util;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * hdfs客户端
 *
 * @author yan_wenjie
 * @date 2021/6/29
 */
public class HDFSClient {

    public static FileSystem fs;

    public static void main(String[] args) throws Exception {
        mkdir();
    }

    public static void mkdir() throws URISyntaxException, IOException, InterruptedException {
// 集群中namenode地址
        URI uri = new URI("hdfs://hadoop01:8020");

        Configuration conf = new Configuration();

        String user = "hadoop";
        FileSystem fs = FileSystem.get(uri, conf, user);

        fs.mkdirs(new Path("/idea/dir1"));

        fs.close();

    }
}
