package com.ustbyjy.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;

public class HDFSApp {

    public static final String HDFS_URI = "hdfs://vhost1:9001";

    private FileSystem fileSystem = null;
    private Configuration configuration = null;

    @Before
    public void setUp() throws Exception {
        System.out.println("setUp...");
        configuration = new Configuration();
        fileSystem = FileSystem.get(URI.create(HDFS_URI), configuration, "root");
    }

    @Test
    public void mkdir() throws Exception {
        boolean flag = fileSystem.mkdirs(new Path("/hdfs_api/test"));
        System.out.println("result: " + flag);
    }

    @Test
    public void create() throws Exception {
        FSDataOutputStream output = fileSystem.create(new Path("/hdfs_api/test/a.txt"));
        output.write("hello hadoop".getBytes());
        output.flush();
        output.close();
    }

    @Test
    public void cat() throws Exception {
        FSDataInputStream in = fileSystem.open(new Path("/hdfs_api/test/a.txt"));
        IOUtils.copyBytes(in, System.out, 1024);
        in.close();
    }

    @Test
    public void rename() throws Exception {
        Path oldPath = new Path("/hdfs_api/test/a.txt");
        Path newPath = new Path("/hdfs_api/test/b.txt");
        boolean flag = fileSystem.rename(oldPath, newPath);
        System.out.println("result: " + flag);
    }

    @Test
    public void copyFromLocalFile() throws Exception {
        Path localPath = new Path(getClass().getClassLoader().getResource("").toString() + "hello.txt");
        Path hdfsPath = new Path("/hdfs_api/test/hello.txt");
        fileSystem.copyFromLocalFile(localPath, hdfsPath);
    }

    @Test
    public void copyToLocalFile() throws Exception {
        Path localPath = new Path(getClass().getClassLoader().getResource("").toString() + "hello_download.txt");
        Path hdfsPath = new Path("/hdfs_api/test/hello.txt");
        fileSystem.copyToLocalFile(false, hdfsPath, localPath, true);
    }

    @Test
    public void listFiles() throws Exception {
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/"));
        for (FileStatus fileStatus : fileStatuses) {
            System.out.println(fileStatus);
        }
    }

    @Test
    public void delete() throws Exception {
        fileSystem.delete(new Path("/hdfs_api/test/"), true);
    }

    @After
    public void tearDown() throws Exception {
        System.out.println("tearDown...");
        fileSystem = null;
        configuration = null;
    }

}
