package com.didichuxing.algo.impl;

import com.didichuxing.algo.AlgoTemplate;
import com.didichuxing.utils.LogUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class algoNull implements AlgoTemplate {

    /**
     * get log output
     */
    private LogUtil logutil;
    /**
     * get logger
     */
    private Logger logger = LoggerFactory.getLogger(AlgoTemplate.class);

    public algoNull(String path) {
        this.logutil = LogUtil.getLogUtil(path);
    }

    @Override
    public void hdfsLs(String srcPath, String dstPath) {
        try {
            FileSystem fileSystem = new Path(dstPath).getFileSystem(conf);
            FileStatus fileStatus = fileSystem.getFileStatus(new Path(dstPath));
            if(fileStatus == null){
                String line = srcPath + " " + dstPath;
                logutil.writelog(line);
                System.out.println(fileSystem.toString());
            }
        } catch (Exception e) {
            String line = srcPath + " " + dstPath;
            logutil.writelog(line);
        }
    }

    @Override
    public void hdfsLsSub(String srcPath, String dstPath) {
        System.out.println("nothing to do");
    }
}
