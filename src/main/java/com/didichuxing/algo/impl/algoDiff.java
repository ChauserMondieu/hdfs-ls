package com.didichuxing.algo.impl;

import com.didichuxing.algo.AlgoTemplate;
import com.didichuxing.utils.LogUtil;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class algoDiff implements AlgoTemplate {

    /**
     * get log output
     */
    private LogUtil logutil;
    /**
     * get logger
     */
    private Logger logger = LoggerFactory.getLogger(AlgoTemplate.class);


    public algoDiff(String path) {
        this.logutil = LogUtil.getLogUtil(path);
    }

    @Override
    public void hdfsLs(String srcPath, String dstPath) {
        try {
            // iteration through each sub directory
            hdfsLsSub(srcPath, dstPath);
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Error|" + srcPath);
        }
    }

    @Override
    public void hdfsLsSub(String path, String dstPath) {
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
                        hdfsLsSub(item.getPath().toString(), dstPath);
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("Error| " + path + " " + dstPath);
        }
    }
}
