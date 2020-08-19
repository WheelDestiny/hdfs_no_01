package com.wheelDestiny.hdfsclient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Array;
import java.net.URI;
import java.util.Arrays;

public class HDFSClient {
    private FileSystem fileSystem;

    @Before
    public void before() throws IOException, InterruptedException {
        //获取一个HDFS封装的对象
        fileSystem = FileSystem.get(URI.create("hdfs://hadoop102:9000"),new Configuration(),"wheelDestiny");
    }

    @Test
    public void put() throws IOException, InterruptedException {
        //获取一个HDFS封装的对象
        FileSystem fileSystem = FileSystem.get(URI.create("hdfs://hadoop102:9000"),new Configuration(),"wheelDestiny");

//        fileSystem.copyFromLocalFile(new Path("D:\\LearnHDFS.txt"),new Path("/"));
        //利用该对象调用HDFS的文件操作系统接口
        fileSystem.copyToLocalFile(new Path("/LearnHDFS.txt"),new Path("D:\\Tools"));

        //关闭接口
        fileSystem.close();
    }
    @Test
    public void rename() throws IOException, InterruptedException {
        //获取文件系统
        FileSystem fileSystem = FileSystem.get(URI.create("hdfs://hadoop102:9000"),new Configuration(),"wheelDestiny");

        //操作
        fileSystem.rename(new Path("/LearnHDFS.txt"),new Path("/LearnHDFS11.txt"));

        //关闭接口
        fileSystem.close();

    }

    @Test
    public void delete() throws IOException, InterruptedException {
        Boolean res = fileSystem.delete(new Path("/bbb"),true);
        System.out.println(res);
    }
    @Test
    public void append() throws IOException, InterruptedException {
        FSDataOutputStream append = fileSystem.append(new Path("/LearnHDFS11.txt"),1024);
        FileInputStream open =  new FileInputStream("D:\\HDFS.txt");
        IOUtils.copyBytes(open,append,1024,true);
    }

    @Test
    public void ls() throws IOException, InterruptedException {
        FileStatus[] fileStatuses =  fileSystem.listStatus(new Path("/"));
        Arrays.stream(fileStatuses).forEach(System.out::println);
    }
    @After
    public void after() throws IOException {
        //关闭接口
        fileSystem.close();
    }

}
