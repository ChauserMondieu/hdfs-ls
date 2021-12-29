package com.didichuxing.algo.impl;

import com.didichuxing.algo.AlgoTemplate;
import com.didichuxing.utils.LogUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class algoRoot implements AlgoTemplate {

    /**
     * get log output
     */
    private LogUtil logutil;
    /**
     * get logger
     */
    private Logger logger = LoggerFactory.getLogger(AlgoTemplate.class);

    public algoRoot(String path) {
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
    public void hdfsLsSub(String srcPath, String dstPath) {
        try {
            // first get root path file status
            FileSystem fs = new Path(srcPath).getFileSystem(conf);
            FileStatus fileStatus = fs.getFileStatus(new Path(srcPath));
            if (fileStatus != null) {
                if (fileStatus.isDirectory()) {
                    String line = fileStatus.getPath().toString() + " "
                            + new Path(dstPath).toUri().getScheme()
                            + "://" + new Path(dstPath).toUri().getAuthority()
                            + fileStatus.getPath().toUri().getRawPath();
                    // write into output file
                    logutil.writelog(line);
                }
            }
            // iteration through each sub directory
            hdfsLsSub(srcPath, dstPath);
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Error|" + srcPath);
        }
    }
}
