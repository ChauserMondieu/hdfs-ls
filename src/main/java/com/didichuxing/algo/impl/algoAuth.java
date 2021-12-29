package com.didichuxing.algo.impl;

import com.didichuxing.algo.AlgoTemplate;
import com.didichuxing.utils.LogUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class algoAuth implements AlgoTemplate {

    /**
     * get log output
     */
    private LogUtil logutil;
    /**
     * get logger
     */
    private Logger logger = LoggerFactory.getLogger(AlgoTemplate.class);

    /**
     * constructor
     * @param path
     */
    public algoAuth(String path) {
        this.logutil = LogUtil.getLogUtil(path);
    }

    /**
     * switch 2
     * used for auth
     * authentication modifying entrance
     *
     * @param srcPath src for example nmg
     * @param dstPath dst for example wh
     */
    @Override
    public void hdfsLs(String srcPath, String dstPath) {
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
            hdfsLsSub(dstPath, srcPath);
        } catch (Exception e) {
            e.printStackTrace();
            msg = "Error|" + srcPath;
            System.err.println(msg);
            logger.info("Error| {}", srcPath);
            logutil.writelog(msg);
        }
    }

    @Override
    public void hdfsLsSub(String iterPath, String stalePath) {
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
                        hdfsLsSub(item.getPath().toString(), stalePath);
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
