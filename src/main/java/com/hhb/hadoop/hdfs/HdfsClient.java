package com.hhb.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author: huanghongbo
 * @Date: 2020-07-02 14:47
 * @Description:
 */
public class HdfsClient {

    private FileSystem fileSystem = null;

    private Configuration configuration = null;

    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        // 1. 获取Hadoop集群configuration对象
        configuration = new Configuration();
        //设置客户端访问datanode使用hostname来进行访问
        configuration.set("dfs.client.use.datanode.hostname", "true");
        // 设置副本数量,生效的优先级，代码里的  >  配置文件 > hadoop-hdfs.jar 里的默认hdfs-default.xml配置
//        configuration.set("dfs.replication", "2");
        // 2. 根据configuration对象获取FileSystem对象
        fileSystem = FileSystem.get(new URI("hdfs://linux121:9000"), configuration, "root");

    }

    @After
    public void destroy() throws IOException {
        fileSystem.close();
    }


    /**
     * 创建目录
     *
     * @throws URISyntaxException
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void testMkdirs() throws URISyntaxException, IOException, InterruptedException {
//        // 1. 获取Hadoop集群configuration对象
//        Configuration configuration = new Configuration();
//        configuration.set("fs.defaultFs", "hdfs://linux121:9000");
//        // 2. 根据configuration对象获取FileSystem对象
//        FileSystem fileSystem = FileSystem.get(new URI("hdfs://linux121:9000"), configuration, "root");
        // 3. 使用FileSystem对象创建一个测试目录
        boolean mkdirs = fileSystem.mkdirs(new Path("/api_test"));
        System.err.println(mkdirs);
        // 4. 释放FileSystem对象
//        fileSystem.close();
    }


    /**
     * 创建目录
     *
     * @throws IOException
     */
    @Test
    public void testMkdirs2() throws IOException {
        // 1. 获取Hadoop集群configuration对象
//        Configuration configuration = new Configuration();
//        configuration.set("fs.defaultFS", "hdfs://linux121:9000");
//        // 2. 根据configuration对象获取FileSystem对象
//        FileSystem fileSystem = FileSystem.get(configuration);
        // 3. 使用FileSystem对象创建一个测试目录
        boolean mkdirs = fileSystem.mkdirs(new Path("/api_test"));
        System.err.println(mkdirs);
        // 4. 释放FileSystem对象
//        fileSystem.close();
    }


    /**
     * 上传文件
     */
    @Test
    public void copyFromLocalToHDFS() throws IOException {
        fileSystem.copyFromLocalFile(new Path("/Users/baiwang/Desktop/yarn-site.xml"), new Path("/api_test"));
    }


    /**
     * 下载文件
     */
    @Test
    public void copyFromHDFSToLocal() throws IOException {
        // 第一个参数：表示是否删除源文件
        fileSystem.copyToLocalFile(true, new Path("/api_test/yarn-site.xml"), new Path("/Users/baiwang/Desktop/"));
    }

    /**
     * 删除文件
     *
     * @throws IOException
     */
    @Test
    public void deleteFile() throws IOException {
        //第二个参数表示是否递归删除
        fileSystem.delete(new Path("/api_test"), true);
    }


    /**
     * 遍历HDFS的根目录，得到文件以及文件夹信息：名称，权限，长度
     *
     * @throws IOException
     */
    @Test
    public void listsFile() throws IOException {
        //第二个参数表示是否递归，该方法返回的一个迭代器，里面存放的是文件状态信息
        RemoteIterator<LocatedFileStatus> remoteIterator = fileSystem.listFiles(new Path("/"), true);
        while (remoteIterator.hasNext()) {
            LocatedFileStatus fileStatus = remoteIterator.next();
            //文件名称
            String name = fileStatus.getPath().getName();
            //文件大小
            long len = fileStatus.getLen();
            //获取权限
            FsPermission permission = fileStatus.getPermission();
            //获取分组信息
            String group = fileStatus.getGroup();
            //获取所属用户
            String owner = fileStatus.getOwner();
            System.err.println(name + "\t" + len + "\t" + permission + "\t" + group + "\t" + owner);
            //块信息
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            for (BlockLocation blockLocation : blockLocations) {
                String[] hosts = blockLocation.getHosts();
                for (String host : hosts) {
                    System.err.println("主机名称：" + host);
                }
            }
            System.err.println("============");

        }
    }

    /**
     * 判读是文件还是文件夹
     *
     * @throws IOException
     */
    @Test
    public void isFile() throws IOException {
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/"));
        for (FileStatus fileStatus : fileStatuses) {
            boolean file = fileStatus.isFile();
            if (file) {
                System.err.println("文件：" + fileStatus.getPath().getName());
            } else {
                System.err.println("文件夹：" + fileStatus.getPath().getName());
            }
        }
    }


    /**
     * 使用IO流操作HDFS
     */
    @Test
    public void uploadFileIO() throws IOException {
        //1. 获取本地输入流
        FileInputStream fileInputStream = new FileInputStream(new File("/Users/baiwang/Desktop/yarn-site.xml"));
        //2. 获取写数据的输出流
        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path("/lagou.txt"));
        //3. 输入流数据拷贝到输出流,copyBytes方法里有默认拷贝值大小（4096），也有是否关闭流的操作（true）
        IOUtils.copyBytes(fileInputStream, fsDataOutputStream, configuration);
    }


    /**
     * 使用IO流下载文件
     */
    @Test
    public void downLoadFileIO() throws IOException {
        //1. 获取HDFS输入流
        FSDataInputStream fsDataInputStream = fileSystem.open(new Path("/lagou.txt"));
        //2. 获取写数据的输出流
        FileOutputStream fileOutputStream = new FileOutputStream(new File("/Users/baiwang/Desktop/hhb.xml"));
        //3. 输入流数据拷贝到输出流,copyBytes方法里有默认拷贝值大小（4096），也有是否关闭流的操作（true）
        IOUtils.copyBytes(fsDataInputStream, fileOutputStream, configuration);
    }


    /**
     * seek 定位读取,使用IO流把lagou.txt
     */
    @Test
    public void seekReadFile() throws IOException {
        //获取HDFS输入流
        FSDataInputStream fsDataInputStream = fileSystem.open(new Path("/lagou.txt"));
        //将输入流复制到控制台中，false表示不会自动关闭流
        IOUtils.copyBytes(fsDataInputStream, System.out, 4096, false);
        //将输入流复制到控制台中。游标在最后的位置，现在将游标指向最前面
        fsDataInputStream.seek(0);
        //将输入流复制到控制台中，false表示不会自动关闭流
        IOUtils.copyBytes(fsDataInputStream, System.out, 4096, false);
        //关闭输入流
        fsDataInputStream.close();
    }


    /**
     * 验证上传文件是以packet为单位
     * 现象：
     * 当 hhb.xml 中无信息，不输出===
     * 当 hhb.xml 中信息大于0KB，小于64KB，输出两个 ===
     * 当 hhb.xml 中信息大于64KB，小于128KB，输出三个 ===
     * 原因：
     * 当hhb.xml中无信息的时候，不会与NameNode交互，所以不会输出，当产生交互的时候，就会先输出一个====。
     * 然后每次上传64KB，所小于64KB的时候会输出两个。一个是建立链接时候的输出，一个是上传数据的输出
     *
     * @throws IOException
     */
    @Test
    public void testUploadFile() throws IOException {
        FileInputStream fileInputStream = new FileInputStream(new File("/Users/baiwang/Desktop/hhb.xml"));
        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path("/lagou.txt"), () -> {
                    System.err.println("===");
                }
        );
        IOUtils.copyBytes(fileInputStream, fsDataOutputStream, configuration);
    }


}
