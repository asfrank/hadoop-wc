package com.imooc.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;
import java.util.concurrent.ConcurrentNavigableMap;

/**
 * Hadoop HDFS java API操作
 */
public class HDFSApp {

    public static final String HDFS_PATH = "hdfs://192.168.6.130:8020";

    FileSystem fileSystem = null;
    Configuration configuration = null;

    @Before
    public void setUp() throws Exception{
        System.out.println("HDFSApp setup");
        configuration = new Configuration();
        fileSystem = FileSystem.get(new URI(HDFS_PATH), configuration, "root");
    }

    //创建hdfs目录
    @Test
    public void mkdir() throws Exception{
        fileSystem.mkdirs(new Path("/hdfsapi/test"));
    }

    //创建文件
    @Test
    public void create() throws Exception{
        FSDataOutputStream outputStream = fileSystem.create(new Path("/hdfsapi/a.txt"));
        outputStream.write("hello hadoop1111".getBytes());
        outputStream.flush();
        outputStream.close();
    }

    //查看文件
    @Test
    public void cat() throws Exception{
        FSDataInputStream in = fileSystem.open(new Path("/hdfsapi/a.txt"));
        IOUtils.copyBytes(in, System.out, 1024);
        in.close();
    }

    //重命名
    @Test
    public void rename() throws Exception{
        Path oldPath = new Path("/hdfsapi/a.txt");
        Path newPath = new Path("/hdfsapi/b.txt");
        fileSystem.rename(oldPath, newPath);
    }

    @Test
    public void copyFromLocalFile() throws Exception{
        Path localPath = new Path("C:\\apk\\1901.apk");
        Path hdfsPath = new Path("/hdfsapi");
        fileSystem.copyFromLocalFile(localPath, hdfsPath);
    }

    @Test
    public void copyFromLocalFileWithProgress() throws Exception{
        InputStream in = new BufferedInputStream(new FileInputStream(new File("C:\\apk\\1901.apk")));
        FSDataOutputStream outputStream = fileSystem.create(new Path("/hdfsapi/test/1111.apk"), new Progressable() {
            @Override
            public void progress() {
                System.out.println(".");
            }
        });
        IOUtils.copyBytes(in, outputStream, 4096);
    }

    //查看目录下的文件
    @Test
    public void listFiles() throws Exception{
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/hdfsapi/test"));
        for (FileStatus fileStatus:fileStatuses){
            String isDir = fileStatus.isDirectory() ? "文件夹" : "文件";
            short replication = fileStatus.getReplication();
            long len = fileStatus.getLen();
            String path = fileStatus.getPath().toString();
            System.out.println(isDir + "\t" + replication + "\t" +len+"\t"+path);
        }
    }

    @Test
    public void delete() throws Exception{
        fileSystem.delete(new Path("/hdfsapi/b.txt"),true);
    }

    @After
    public void tearDown() throws Exception{
        configuration = null;
        fileSystem = null;
        System.out.println("HDFSApp teardown");
    }
}
