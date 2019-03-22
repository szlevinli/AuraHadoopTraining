package org.training.hadoop.hdfs;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;

import java.net.URI;

public class HdfsMyExample {
    public static void main(String[] args) {
        try {
            runAll();
        } catch (Exception e) {
            System.out.println("Exceptions:" + e);
        } finally {
            System.out.println("timestamp:" + System.currentTimeMillis());
        }
    }

    public static void runAll() throws Exception {
        String hdfsUri = "hdfs://192.168.12.158:9000";
        String path = "/test";
        String fileName = "hello.txt";
        String fileContent = "hello world!";

        // ===== Init HDFS File System Object
        System.out.println("start init HDFS file system object...");
        Configuration conf = new Configuration();
        // Set FileSystem URI
        conf.set("fs.defaultFS", hdfsUri);
        // Because Maven
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        // Set Hadoop User
        System.setProperty("HADOOP_USER_NAME", "bigdata");
        System.setProperty("hadoop.home.dir", "/");
        // Get HDFS FileSystem Object
        FileSystem fs = FileSystem.get(URI.create(hdfsUri), conf);

        // ===== Create Folder if not exists
        System.out.println("start create folder...");
        Path workingPath = fs.getWorkingDirectory();
        Path newFolderPath = new Path(path);
        if (!fs.exists(newFolderPath)) {
            // create folder
            fs.mkdirs(newFolderPath);
        }

        // ===== Write file
        System.out.println("start write file...");
        // create path
        Path hdfsWritePath = new Path(newFolderPath + "/" + fileName);
        // init output stream
        FSDataOutputStream fOut = fs.create(hdfsWritePath);
        // classical output stream usage
        fOut.writeBytes(fileContent);
        fOut.close();

        // ===== Read file
        System.out.println("start read file...");
        // create path
        Path hdfsReadPath = new Path(newFolderPath + "/" + fileName);
        // init input stream
        FSDataInputStream fIn = fs.open(hdfsReadPath);
        // classical input stream usage
        String content = IOUtils.toString(fIn, "UTF-8");
        fIn.close();
        fs.close();
    }
}
