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
        String hdfsUri = "hdfs://10.0.0.114:9000";
        String path = "/test";
        String fileName = "hello.txt";
        String fileContent = "hello world!";

        FileSystem fs = null;
        FSDataInputStream fIn = null;
        FSDataOutputStream fOut = null;

        try {
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
            fs = FileSystem.get(URI.create(hdfsUri), conf);

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
            fOut = fs.create(hdfsWritePath);
            // classical output stream usage
            fOut.writeBytes(fileContent);
            fOut.close();

            // ===== Read file
            System.out.println("start read file...");
            // create path
            Path hdfsReadPath = new Path(newFolderPath + "/" + fileName);
            // init input stream
            fIn = fs.open(hdfsReadPath);
            // classical input stream usage
            String content = IOUtils.toString(fIn, "UTF-8");
            System.out.println("file contents is " + content);
            fIn.close();

            // ===== Delete file
            System.out.println("start delete file...");
            // create path
            Path hdfsDeletePath = new Path(newFolderPath + "/" + fileName);
            boolean isRecursive = true;
            fs.delete(hdfsDeletePath, isRecursive);
        } catch (Exception e) {
            System.out.println("Exception:" + e);
        } finally {
            try {
                if (fs != null) fs.close();
                if (fIn != null) fIn.close();
                if (fOut != null) fOut.close();
            } catch (Exception ignored) {
                // nothing to do.
            }
        }
    }
}
