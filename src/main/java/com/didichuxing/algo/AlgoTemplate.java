package com.didichuxing.algo;

import com.didichuxing.conf.LsConfiguration;
import org.apache.hadoop.conf.Configuration;

public interface AlgoTemplate {

     Configuration conf = LsConfiguration.getConf();

    /**
     * 完成根文件夹的操作
     * @param srcPath
     * @param dstPath
     * @return void
     */
    void hdfsLs(String srcPath, String dstPath);

    /**
     * 完成跟文件夹下各子文件夹的操作
     * @param srcPath
     * @param dstPath
     */
    void hdfsLsSub(String srcPath, String dstPath);
}
