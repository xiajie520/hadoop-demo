package com.wenjie.hdfs.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;


public class HDFSClientTest {
    /**
     * hdfs客户端
     */
    private FileSystem fs;

    /**
     * Test前置操作
     *
     * @throws Exception
     */
    @Before
    public void init() throws Exception {
        URI uri = new URI("hdfs://hadoop01:8020");

        /**
         * 配置优先级 由低到高：hdfs-default.xml -> 环境中的hdfs-site.xml ->程序配置中的hdfs-site.xml -> 代码中编写的conf
         */
        Configuration conf = new Configuration();

        String user = "hadoop";
        fs = FileSystem.get(uri, conf, user);
    }

    /**
     * Test后置操作
     *
     * @throws IOException
     */
    @After
    public void close() throws IOException {
        fs.close();
    }

    @Test
    public void mkdir() throws IOException {
        fs.mkdirs(new Path("/idea/dir2"));
    }

    /**
     * 文件上传测试
     *
     * @throws IOException
     */
    @Test
    public void putTest() throws IOException {
//        是否删除源文件
        boolean delSrc = false;
//        如目标路径已经有文件了，是否覆盖
        boolean overwrite = true;
        // 源文件路径
        Path src = new Path("J:\\软件\\typora（MD）.exe");
        // 目标文件路径（这里有个坑）
        /**
         * 举例，如/idea/dir2这个文件夹不存在，上面的文件会直接上传到/idea文件夹，并且重命名为dir2,你就得到了一个名为dir2的文件
         * 如果文件夹存在，则会将文件上传到文件夹下（哪怕你用/idea/dir2/也是一样的结果）
         */
        Path dst = new Path("/idea/dir2");
        fs.copyFromLocalFile(delSrc, overwrite, src, dst);
    }

    /**
     * 文件下载测试
     */
    @Test
    public void getTest() throws IOException {
        // 源文件是否删除
        boolean delSrc = false;
        // 源文件路径
        Path src = new Path("/idea/dir2/typora（MD）.exe");
        // 目标文件路径（本地路径）
        Path dst = new Path("C:\\Users\\86139\\Desktop\\hadoop");
        // 是否开启本地校验 如果是false ,下载完毕之后除了源文件之外还有一个crc格式的文件，是做校验用的
        //
        boolean useRawLocalFileSystem = false;
        fs.copyToLocalFile(delSrc, src, dst, useRawLocalFileSystem);
    }

    /**
     * 删除测试
     *
     * @throws IOException
     */
    @Test
    public void delTest() throws IOException {
        // 需要删除的文件
        Path path = new Path("/idea/dir2");
        // 是否递归删除 ,删除空目录或者删除文件的时候可以是false，删除非空目录需要设置为true
        boolean isr = true;
        fs.delete(path, isr);
    }
}