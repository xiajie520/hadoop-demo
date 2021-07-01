package com.wenjie.hdfs.util;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;


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

    /**
     * 测试更名和移动
     *
     * @throws Exception
     */
    @Test
    public void mvTest() throws Exception {
        Path path = new Path("/idea/input5");
        // 第一个是源文件或文件夹，第二个是目标文件或文件夹
        // fs移动时会截取最后一段名称作为文件名或者文件夹的名称，如目标是文件夹名称，则会置入文件夹，如目标名称不存在，则会更名为目标名称
        // 与linux中的mv命令大概相同
        fs.rename(path, new Path("/idea/dir1/tt"));
    }

    /**
     * 查看文件信息
     *
     * @throws Exception
     */
    @Test
    public void getFileInfo() throws Exception {
        // 两个参数   final Path f 文件路径, final boolean recursive 是否递归++

        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
        while (listFiles.hasNext()) {
            LocatedFileStatus next = listFiles.next();
            System.out.println("=====" + next.getPath() + "=====");
            /**
             * 参数
             *     private static final long serialVersionUID = 332065512L;
             *     private Path path; 路径
             *     private long length; 文件大小
             *     private Boolean isdir; 是否为文件夹
             *     private short block_replication;
             *     private long blocksize; 文件块大小
             *     private long modification_time; 更新时间
             *     private long access_time; 上传时间
             *     private FsPermission permission; 权限
             *     private String owner; 文件所属人
             *     private String group; 文件所属组
             *     private Path symlink; 符号链接
             *     private Set<FileStatus.AttrFlags> attr; 属性标志
             *     public static final Set<FileStatus.AttrFlags> NONE = Collections.emptySet();
             *     private BlockLocation[] locations; 所有块信息
             */
            System.out.println(next.getReplication());
            System.out.println(next.getAccessTime());
            if (next.isSymlink()) {
                System.out.println(next.getSymlink());
            }
            System.out.println(Arrays.toString(next.getBlockLocations()));


        }
    }

}