package com.didichuxing.utils;

import com.didichuxing.pojo.Data;
import com.google.common.collect.Lists;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


import java.io.*;
import java.util.List;
import java.util.Scanner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HdfsLs{

    /**
     * hdfs configuration - singleton
     */
    private static Configuration conf;
    /**
     *  build-in log writer
     */
    private static LogUtil logutil;
    /**
     * structured log writer
     */
    private static Logger logger = LoggerFactory.getLogger(HdfsLs.class);


    public static void main(String[] args) {
        // initialize new scanner
        Scanner scanner = new Scanner(System.in);

        // first input corresponding logpath to store
        System.out.println("first input file path");
        String logpath = scanner.nextLine();
        logutil = new LogUtil(logpath);

        // then decides which mode to choose
        System.out.println("then input one param indicate mode, " +
                "1 for diff, 2 for auth;");
        int flag = scanner.nextInt();
        scanner.nextLine();

        HdfsLs hdfsDiff = null;
        String path = "";
        String inputStr = "";
        String[] inputStrs = null;

        switch (flag) {
            case 1:
                System.out.println("please input 1 param, " +
                        "which is the file contains [ src dst ] ");
                // first make sure input is valid
                inputStr = scanner.nextLine();
                inputStrs = inputStr.split("\\s+");
                if (inputStrs.length < 1 || inputStrs.length > 2) {
                    System.err.println("invalid inputs, number of inputs should be exact 1");
                }
                path = inputStrs[0];
                hdfsDiff = new HdfsLs();
                hdfsDiff.getConf();
                List<Data> list = hdfsDiff.infos(path);
                for (Data data : list) {
                    hdfsDiff.ls(data.getSrc(), data.getDest());
                }
                break;
            case 2:
                System.out.println("please input 1 params, " +
                        "first is the file contains src dst");
                // first make sure input is valid
                inputStr = scanner.nextLine();
                inputStrs = inputStr.split("\\s+");
                if (inputStrs.length < 1 || inputStrs.length > 2) {
                    System.err.println("invalid inputs, number of inputs should be exact 1");
                }
                path = inputStrs[0];
                hdfsDiff = new HdfsLs();
                hdfsDiff.getConf();
                List<Data> list2 = hdfsDiff.infos(path);
                for (Data data : list2) {
                    hdfsDiff.authModify(data.getSrc(), data.getDest());
                }
                break;
            default:
                break;
        }
        logutil.close();
    }

    /**
     * get hdfs configuration - singleton
     * @return
     */
    public Configuration getConf() {
        if (conf == null) {
            synchronized (this) {
                if (conf == null) {
                    conf = new Configuration();
                    conf.addResource("core-site.xml");
                    conf.addResource("hdfs-site.xml");
                    conf.set("HADOOP_USER_NAME", "${HADOOP_USER_NAME}");
                    conf.set("HADOOP_USER_PASSWORD", "${HADOOP_USER_PASSWORD}");
                }
            }
        }
        return conf;
    }

    /**
     * switch 1
     * make sure diff function
     * all outputs are exported into files, and errors into console
     * @param srcPath src path for exapmle nmg
     * @param dstPath dst path for example wh
     */
    public void ls(String srcPath, String dstPath) {
        try {
            // iteration through each sub directory
            lsSubRecursively(srcPath, dstPath);
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Error|" + srcPath);
        }
    }

    /**
     * switch 1
     * used for recursively iterate files under one root file
     * in this function, we only iterate srcpath, and just append relative path of each file after dstpath
     *
     * @param path
     */
    private void lsSubRecursively(String path, String dstPath) throws IOException, IllegalArgumentException {
        try {
            // instantiate only one writer for writing into output file
            FileSystem fs = new Path(path).getFileSystem(conf);
            FileStatus[] fileStatus = fs.listStatus(new Path(path));
            if (null != fileStatus && fileStatus.length > 0) {
                for (FileStatus item : fileStatus) {
                    if (item.isDirectory()) {
                        String line = item.getPath().toString() + " "
                                + new Path(dstPath).toUri().getScheme()
                                + "://" + new Path(dstPath).toUri().getAuthority()
                                + item.getPath().toUri().getRawPath();
                        // write into output file
                        logutil.writelog(line);
                        // list recursively
                        lsSubRecursively(item.getPath().toString(), dstPath);
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("Error| " + path + " " + dstPath);
        }
    }

    /**
     * switch 2
     * used for auth
     * authentication modifying entrance
     *
     * @param srcPath src for example nmg
     * @param dstPath dst for example wh
     */
    public void authModify(String srcPath, String dstPath) {
        // used for log printing
        String msg = "";
        try {
            // iter for example wh
            String iterPath = dstPath;
            // stale for example nmg
            String stalePath = srcPath;
            FileSystem iterFs = new Path(iterPath).getFileSystem(conf);
            FileSystem staleFs = new Path(stalePath).getFileSystem(conf);
            FileStatus fs = iterFs.getFileStatus(new Path(iterPath));
            if (null != fs) {
                String src = new Path(stalePath).toUri().getScheme()
                        + "://" + new Path(stalePath).toUri().getAuthority()
                        + fs.getPath().toUri().getRawPath();
                // gain src auth
                // here src stands for src, for example nmg
                if (staleFs.exists(new Path(srcPath))) {
                    String srcOwner = staleFs.getFileStatus(new Path(srcPath)).getOwner();
                    String srcGroup = staleFs.getFileStatus(new Path(srcPath)).getGroup();
                    // gain dst path
                    // here dst stands for dst, for example wh
                    String dst = fs.getPath().toString();
                    iterFs = new Path(dstPath).getFileSystem(conf);
                    if (iterFs.exists(new Path(dstPath))) {
                        iterFs.setOwner(new Path(dstPath), srcOwner, srcGroup);
                        msg = "Success| " + src + " " + dst;
                        System.out.println(msg);
                        logger.info("Success| {} {}", src, dst);
                        logutil.writelog(msg);
                    } else {
                        msg = "Error| no corresponding dst file! " + dst;
                        System.err.println(msg);
                        logger.info("Error| no corresponding dst file! {}", dst);
                        logutil.writelog(msg);
                    }
                } else {
                    msg = "Error| no corresponding src file! " + src;
                    System.err.println(msg);
                    logger.info("Error| no corresponding src file! {}", src);
                    logutil.writelog(msg);
                }
            }
            // dst - wh
            // src - nmg
            authModifySubRecursively(dstPath, srcPath);
        } catch (Exception e) {
            e.printStackTrace();
            msg = "Error|" + srcPath;
            System.err.println(msg);
            logger.info("Error| {}", srcPath);
            logutil.writelog(msg);
        }
    }

    /**
     * switch 2 userd for auth
     * @param iterPath:  stands for dst filesystem, for example wh
     * @param stalePath: stands for src filesystem, for example nmg
     * @throws IOException
     * @throws IllegalArgumentException
     */
    private void authModifySubRecursively(String iterPath, String stalePath) throws IOException, IllegalArgumentException {
        String msg;
        try {
            // instantiate only one writer for writing into output file
            FileSystem iterFs = new Path(iterPath).getFileSystem(conf);
            FileStatus[] fileStatus = iterFs.listStatus(new Path(iterPath));
            FileSystem staleFs = new Path(stalePath).getFileSystem(conf);
            String srcOwner = "";
            String srcGroup = "";
            if (null != fileStatus && fileStatus.length > 0) {
                for (FileStatus item : fileStatus) {
                    if (item.isDirectory()) {
                        String srcPath = new Path(stalePath).toUri().getScheme()
                                + "://" + new Path(stalePath).toUri().getAuthority()
                                + item.getPath().toUri().getRawPath();
                        // gain src auth
                        // here src stands for src, for example nmg
                        if (staleFs.exists(new Path(srcPath))) {
                            srcOwner = staleFs.getFileStatus(new Path(srcPath)).getOwner();
                            srcGroup = staleFs.getFileStatus(new Path(srcPath)).getGroup();
                            // gain dst path
                            // here dst stands for dst, for example wh
                            String dstPath = item.getPath().toString();
                            iterFs = new Path(dstPath).getFileSystem(conf);
                            if (iterFs.exists(new Path(dstPath))) {
                                iterFs.setOwner(new Path(dstPath), srcOwner, srcGroup);
                                msg = "Success| " + srcPath + " " + dstPath;
                                System.out.println(msg);
                                logger.info("Success| {} {}", srcPath, dstPath);
                                logutil.writelog(msg);
                            } else {
                                msg = "Error| no corresponding dst file! " + dstPath;
                                System.err.println(msg);
                                logger.info("Error| no corresponding dst file! {}", dstPath);
                                logutil.writelog(msg);
                            }
                        } else {
                            msg = "Error| no corresponding src file! " + srcPath;
                            System.err.println(msg);
                            logger.info("Error| no corresponding src file! {}", srcPath);
                            logutil.writelog(msg);
                        }
                        authModifySubRecursively(item.getPath().toString(), stalePath);
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * separate src & dst from sigle input file
     * @param fileName
     * @return
     */
    public List<Data> infos(String fileName) {
        File file = new File(fileName);
        List<Data> lists = Lists.newArrayList();
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(file));
            String temp = null;
            Data data = null;
            int line = 1;
            // 一次读入一行，直到读入null为文件结束
            while ((temp = reader.readLine()) != null) {
                // 显示行号
                String[] temps = temp.split("\\s+");
                if (temps.length == 2) {
                    data = new Data(temps[0], temps[1]);
                    lists.add(data);
                } else {
                    System.err.println("read error line " + line + ": " + temp + ":" + temps.length);
                }
                line++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException io) {
                    io.printStackTrace();
                }
            }
        }
        return lists;
    }
}


