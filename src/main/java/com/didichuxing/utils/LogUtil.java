package com.didichuxing.utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class LogUtil {

    private static FileWriter fw;
    private static BufferedWriter bw;

    /**
     * get singleton bufferedWriter
     * @param logpath log path
     * @return bw
     */
    public LogUtil(String logpath) {
        if (null == bw) {
            synchronized (this) {
                if (null == bw) {
                    try {
                        fw = new FileWriter(logpath,true);
                        bw = new BufferedWriter(fw);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
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
    public void close() {
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
