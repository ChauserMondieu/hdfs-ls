package com.didichuxing.utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class LogUtil {

    private static FileWriter fw;
    private static BufferedWriter bw;
    
    public static LogUtil getLogUtil(String logPath){
        return new LogUtil(logPath);
    }

    /**
     * get singleton bufferedWriter
     * @param logPath log path
     * @return bw
     */
    private LogUtil(String logPath){
        try {
            fw = new FileWriter(logPath,true);
            bw = new BufferedWriter(fw);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }



    /**
     * self-defined log function
     *
     * @param msg
     */
    public void writelog(String msg) {
        try {
            bw.write(msg + "\n");
            bw.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * close write stream
     */
    public static void close() {
        if (null != fw) {
            try {
                fw.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (null != bw) {
            try {
                bw.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * delete file if already exist
     *
     * @param path
     */
    public void preCheckFile(String path) {
        File file = new File(path);
        try {
            if (file.exists()) {
                System.out.println("file already exists, now deleting it");
                file.delete();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
