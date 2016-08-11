package com.mycom.bds.zk.util;

import java.io.File;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.zookeeper.ZooKeeper;

public class MyZKUtils {

    private static DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");

    /**
     * @param parent
     * @param fileName
     * @return file path with forward slash
     */
    public static String getPath(File parent, String fileName) {
        File file = new File(parent, fileName);
        ensureExists(file);
        String absolutePath = file.getAbsolutePath();
        absolutePath = absolutePath.replace("\\", "/");
        System.out.println("path is " + absolutePath);
        return absolutePath;
    }

    /**
     * throws {@link RuntimeException} if file does not exist
     */
    public static void ensureExists(File file) {
        if (!file.exists()) {
            throw new RuntimeException("File '" + file.getAbsolutePath() + "' does not exist.");
        }
    }

    public static byte[] data() {
        return "test date".getBytes();
    }

    public static void print(String message) {
        String dateMessage = dateFormat.format(new Date());
        System.out.println(dateMessage + "::" + message);

    }

    public static String getConnectedServer(ZooKeeper zk) {
        String result = "remoteserver:None";
        String connectionString = zk.toString();
        int indexOfRemoteserver = connectionString.indexOf("remoteserver:");
        if (-1 == indexOfRemoteserver) {
            return result;
        }
        int indexOfSpace = connectionString.indexOf(" ", indexOfRemoteserver);
        if (-1 == indexOfSpace) {
            return result;
        }
        return connectionString.substring(indexOfRemoteserver, indexOfSpace + 1);
    }
}
